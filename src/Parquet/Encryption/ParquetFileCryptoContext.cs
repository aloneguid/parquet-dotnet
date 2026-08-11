using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using Parquet.Meta;

namespace Parquet.Encryption;

internal sealed class ParquetFileCryptoContext {
    private readonly ParquetEncryptionOptions? _encryptionOptions;
    private readonly ParquetDecryptionOptions? _decryptionOptions;
    private readonly Dictionary<string, ParquetCryptoContext> _columnContexts = new(StringComparer.Ordinal);

    private ParquetFileCryptoContext(
        ParquetCryptoContext footer,
        EncryptionAlgorithm thriftAlgorithm,
        bool encryptedFooter,
        ParquetEncryptionOptions? encryptionOptions,
        ParquetDecryptionOptions? decryptionOptions) {
        Footer = footer;
        ThriftAlgorithm = thriftAlgorithm;
        EncryptedFooter = encryptedFooter;
        _encryptionOptions = encryptionOptions;
        _decryptionOptions = decryptionOptions;
    }

    public ParquetCryptoContext Footer { get; }

    public EncryptionAlgorithm ThriftAlgorithm { get; }

    public bool EncryptedFooter { get; }

    public byte[]? FooterKeyMetadata => _encryptionOptions?.FooterKey.KeyMetadata;

    public static ParquetFileCryptoContext CreateForWrite(ParquetEncryptionOptions options) {
        ArgumentNullException.ThrowIfNull(options);
        if(!options.StoreAadPrefix && (options.AadPrefix == null || options.AadPrefix.Length == 0))
            throw new ArgumentException("An AAD prefix is required when the prefix is not stored in the file.", nameof(options));

        byte[] fileUnique = RandomNumberGenerator.GetBytes(ParquetCryptoContext.FileUniqueLength);
        byte[] prefix = options.AadPrefix ?? Array.Empty<byte>();
        EncryptionAlgorithm thriftAlgorithm = CreateAlgorithm(
            options.Algorithm,
            options.StoreAadPrefix ? prefix : null,
            fileUnique,
            !options.StoreAadPrefix);
        var footer = new ParquetCryptoContext(options.FooterKey.KeyBytes, prefix, fileUnique, options.Algorithm);
        return new ParquetFileCryptoContext(footer, thriftAlgorithm, options.EncryptFooter, options, null);
    }

    public static ParquetFileCryptoContext CreateForRead(
        EncryptionAlgorithm algorithm,
        byte[]? footerKeyMetadata,
        bool encryptedFooter,
        ParquetDecryptionOptions? options) {
        ArgumentNullException.ThrowIfNull(algorithm);
        (ParquetEncryptionAlgorithm kind, byte[] storedPrefix, byte[] fileUnique, bool supplyPrefix) = ParseAlgorithm(algorithm);
        byte[] prefix;
        if(supplyPrefix) {
            prefix = options?.AadPrefix ?? throw new InvalidDataException(
                "This encrypted file requires an externally supplied AAD prefix.");
        } else {
            prefix = storedPrefix;
        }

        byte[] key = ResolveFooterKey(options, footerKeyMetadata);
        var footer = new ParquetCryptoContext(key, prefix, fileUnique, kind);
        return new ParquetFileCryptoContext(footer, algorithm, encryptedFooter, null, options);
    }

    public ColumnEncryptionContext? GetColumnEncryptionContext(string path) {
        if(_encryptionOptions == null)
            throw new InvalidOperationException("This file context was not created for writing.");

        if(_encryptionOptions.ColumnKeys.TryGetValue(path, out ParquetKey? columnKey)) {
            return new ColumnEncryptionContext(
                GetOrCreateColumnContext(path, columnKey.KeyBytes),
                true,
                columnKey.KeyMetadata);
        }

        if(_encryptionOptions.EncryptAllColumns || _encryptionOptions.FooterKeyColumns.Contains(path))
            return new ColumnEncryptionContext(Footer, false, null);

        return null;
    }

    public ParquetCryptoContext GetColumnDecryptionContext(ColumnChunk chunk, string path) {
        if(chunk.CryptoMetadata?.ENCRYPTIONWITHFOOTERKEY != null)
            return Footer;

        EncryptionWithColumnKey columnKey = chunk.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY
            ?? throw new InvalidDataException($"Encrypted column '{path}' is missing column crypto metadata.");
        if(_columnContexts.TryGetValue(path, out ParquetCryptoContext? existing))
            return existing;

        byte[] key;
        if(_decryptionOptions?.ColumnKeys.TryGetValue(path, out byte[]? directKey) == true) {
            key = directKey;
        } else if(_decryptionOptions?.KeyRetriever != null) {
            key = _decryptionOptions.KeyRetriever
                .GetKey(columnKey.KeyMetadata ?? Array.Empty<byte>())
                .ToArray();
        } else {
            throw new InvalidDataException(
                $"Column '{path}' is encrypted with a column key, but no key was provided.");
        }

        ParquetCryptoContext context = Footer.WithKey(key);
        _columnContexts[path] = context;
        return context;
    }

    private ParquetCryptoContext GetOrCreateColumnContext(string path, byte[] key) {
        if(_columnContexts.TryGetValue(path, out ParquetCryptoContext? existing))
            return existing;
        ParquetCryptoContext context = Footer.WithKey(key);
        _columnContexts[path] = context;
        return context;
    }

    private static byte[] ResolveFooterKey(ParquetDecryptionOptions? options, byte[]? metadata) {
        byte[]? key = options?.FooterKey;
        if(key == null && options?.KeyRetriever != null)
            key = options.KeyRetriever.GetKey(metadata ?? Array.Empty<byte>()).ToArray();
        if(key == null)
            throw new InvalidDataException("A footer key is required to read this encrypted Parquet file.");
        if(key.Length is not (16 or 24 or 32))
            throw new InvalidDataException("The resolved footer key is not a valid AES key.");
        return key;
    }

    private static EncryptionAlgorithm CreateAlgorithm(
        ParquetEncryptionAlgorithm algorithm,
        byte[]? storedPrefix,
        byte[] fileUnique,
        bool supplyPrefix) {
        return algorithm switch {
            ParquetEncryptionAlgorithm.AesGcmV1 => new EncryptionAlgorithm {
                AESGCMV1 = new AesGcmV1 {
                    AadPrefix = storedPrefix is { Length: > 0 } ? storedPrefix : null,
                    AadFileUnique = fileUnique,
                    SupplyAadPrefix = supplyPrefix ? true : null
                }
            },
            ParquetEncryptionAlgorithm.AesGcmCtrV1 => new EncryptionAlgorithm {
                AESGCMCTRV1 = new AesGcmCtrV1 {
                    AadPrefix = storedPrefix is { Length: > 0 } ? storedPrefix : null,
                    AadFileUnique = fileUnique,
                    SupplyAadPrefix = supplyPrefix ? true : null
                }
            },
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm))
        };
    }

    private static (ParquetEncryptionAlgorithm Algorithm, byte[] Prefix, byte[] FileUnique, bool SupplyPrefix)
        ParseAlgorithm(EncryptionAlgorithm algorithm) {
        if(algorithm.AESGCMV1 != null) {
            AesGcmV1 value = algorithm.AESGCMV1;
            return (
                ParquetEncryptionAlgorithm.AesGcmV1,
                value.AadPrefix ?? Array.Empty<byte>(),
                RequireFileUnique(value.AadFileUnique),
                value.SupplyAadPrefix == true);
        }
        if(algorithm.AESGCMCTRV1 != null) {
            AesGcmCtrV1 value = algorithm.AESGCMCTRV1;
            return (
                ParquetEncryptionAlgorithm.AesGcmCtrV1,
                value.AadPrefix ?? Array.Empty<byte>(),
                RequireFileUnique(value.AadFileUnique),
                value.SupplyAadPrefix == true);
        }
        throw new NotSupportedException("The Parquet encryption algorithm is not supported.");
    }

    private static byte[] RequireFileUnique(byte[]? fileUnique) =>
        fileUnique is { Length: > 0 }
            ? fileUnique
            : throw new InvalidDataException("The encrypted file does not contain an AAD file identifier.");
}

internal sealed record ColumnEncryptionContext(
    ParquetCryptoContext Crypto,
    bool UsesColumnKey,
    byte[]? KeyMetadata);
