using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Encryption;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet;

/// <summary>
/// Base class for reader and writer
/// </summary>
public class ParquetActor {
    internal static readonly byte[] MagicBytes = System.Text.Encoding.ASCII.GetBytes("PAR1");
    internal static readonly byte[] EncryptedMagicBytes = System.Text.Encoding.ASCII.GetBytes("PARE");
    private readonly Stream _fileStream;

    internal ParquetActor(Stream? fileStream) =>
        _fileStream = fileStream ?? throw new ArgumentNullException(nameof(fileStream));

    /// <summary>
    /// Original stream to write or read
    /// </summary>
    protected Stream Stream => _fileStream;

    internal bool HasEncryptedFooter { get; private set; }

    /// <summary>
    /// Validates that this file is a valid parquet file by reading head and tail of it
    /// </summary>
    /// <returns></returns>
    /// <exception cref="IOException"></exception>
    protected async Task ValidateFileAsync() {
        _fileStream.Seek(0, SeekOrigin.Begin);
        byte[] head = await _fileStream.ReadBytesExactlyAsync(4);

        _fileStream.Seek(-4, SeekOrigin.End);
        byte[] tail = await _fileStream.ReadBytesExactlyAsync(4);

        bool plaintextFooter = MagicBytes.SequenceEqual(head) && MagicBytes.SequenceEqual(tail);
        bool encryptedFooter = EncryptedMagicBytes.SequenceEqual(head) && EncryptedMagicBytes.SequenceEqual(tail);
        if(!plaintextFooter && !encryptedFooter)
            throw new IOException($"not a parquet file, head: {head.ToHexString()}, tail: {tail.ToHexString()}");
        HasEncryptedFooter = encryptedFooter;
    }

    internal async ValueTask<ParquetMetadataReadResult> ReadMetadataAsync(
        ParquetOptions options,
        CancellationToken cancellationToken = default) {
        int footerLength = await GoBeforeFooterAsync();
        if(footerLength < 0 || footerLength > _fileStream.Length - 8)
            throw new InvalidDataException($"The footer length {footerLength} is invalid.");
        byte[] footerData = await _fileStream.ReadBytesExactlyAsync(footerLength);
        using var ms = new MemoryStream(footerData);

        if(HasEncryptedFooter) {
            FileCryptoMetaData cryptoMetaData = FileCryptoMetaData.Read(new ThriftCompactProtocolReader(ms));
            ParquetFileCryptoContext crypto = ParquetFileCryptoContext.CreateForRead(
                cryptoMetaData.EncryptionAlgorithm,
                cryptoMetaData.KeyMetadata,
                true,
                options.Decryption);
            byte[] plaintextFooter = crypto.Footer.Decrypt(ms, ParquetModuleType.Footer);
            if(ms.Position != ms.Length)
                throw new InvalidDataException("Unexpected bytes follow the encrypted footer.");
            using var footerStream = new MemoryStream(plaintextFooter, writable: false);
            FileMetaData metadata = FileMetaData.Read(new ThriftCompactProtocolReader(footerStream));
            ParquetCryptoContext.ValidateTrailingPadding(
                plaintextFooter,
                footerStream.Position,
                "decrypted footer metadata");
            return new ParquetMetadataReadResult(metadata, crypto);
        }

        FileMetaData plaintextMetadata = FileMetaData.Read(new ThriftCompactProtocolReader(ms));
        if(plaintextMetadata.EncryptionAlgorithm == null)
            return new ParquetMetadataReadResult(plaintextMetadata, null);

        long metadataLength = ms.Position;
        if(ms.Length - metadataLength != ParquetCryptoContext.NonceLength + ParquetCryptoContext.TagLength)
            throw new InvalidDataException("The signed plaintext footer does not contain a valid signature.");

        ParquetFileCryptoContext plaintextCrypto = ParquetFileCryptoContext.CreateForRead(
            plaintextMetadata.EncryptionAlgorithm,
            plaintextMetadata.FooterSigningKeyMetadata,
            false,
            options.Decryption);
        plaintextCrypto.Footer.VerifyFooter(
            footerData.AsSpan(0, checked((int)metadataLength)),
            footerData.AsSpan(checked((int)metadataLength)));
        return new ParquetMetadataReadResult(plaintextMetadata, plaintextCrypto);
    }

    internal async ValueTask<int> GoBeforeFooterAsync() {
        //go to -4 bytes (PAR1) -4 bytes (footer length number)
        _fileStream.Seek(-8, SeekOrigin.End);
        int footerLength = await _fileStream.ReadInt32Async();

        if(footerLength < 0 || footerLength > _fileStream.Length - 8)
            throw new InvalidDataException($"The footer length {footerLength} is invalid.");

        //set just before footer starts
        _fileStream.Seek(-8 - footerLength, SeekOrigin.End);

        return footerLength;
    }
}

internal sealed record ParquetMetadataReadResult(
    FileMetaData Metadata,
    ParquetFileCryptoContext? CryptoContext);
