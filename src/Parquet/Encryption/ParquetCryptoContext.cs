using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using Parquet.Extensions;

namespace Parquet.Encryption;

internal sealed class ParquetCryptoContext {
    internal const int NonceLength = 12;
    internal const int TagLength = 16;
    internal const int FileUniqueLength = 8;

    private readonly byte[] _key;
    private readonly byte[] _aadPrefix;
    private readonly byte[] _aadFileUnique;
    private long _gcmInvocationCount;

    public ParquetCryptoContext(
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> aadPrefix,
        ReadOnlySpan<byte> aadFileUnique,
        ParquetEncryptionAlgorithm algorithm) {
        ValidateKey(key);
        if(aadFileUnique.IsEmpty)
            throw new ArgumentException("The AAD file identifier must not be empty.", nameof(aadFileUnique));

        _key = key.ToArray();
        _aadPrefix = aadPrefix.ToArray();
        _aadFileUnique = aadFileUnique.ToArray();
        Algorithm = algorithm;
    }

    public ParquetEncryptionAlgorithm Algorithm { get; }

    public ReadOnlyMemory<byte> AadPrefix => _aadPrefix;

    public ReadOnlyMemory<byte> AadFileUnique => _aadFileUnique;

    public ParquetCryptoContext WithKey(ReadOnlySpan<byte> key) =>
        new ParquetCryptoContext(key, _aadPrefix, _aadFileUnique, Algorithm);

    public byte[] Encrypt(
        ReadOnlySpan<byte> plaintext,
        ParquetModuleType module,
        short? rowGroupOrdinal = null,
        short? columnOrdinal = null,
        short? pageOrdinal = null) {
        ValidateOrdinals(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        if(Algorithm == ParquetEncryptionAlgorithm.AesGcmCtrV1 &&
           module is ParquetModuleType.DataPage or ParquetModuleType.DictionaryPage) {
            return EncryptCtr(plaintext);
        }

        CountGcmInvocation();
        byte[] nonce = RandomNumberGenerator.GetBytes(NonceLength);
        byte[] ciphertext = new byte[plaintext.Length];
        byte[] tag = new byte[TagLength];
        byte[] aad = BuildAad(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        using(var gcm = new AesGcm(_key, TagLength)) {
            gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);
        }
        return Frame(nonce, ciphertext, tag);
    }

    public byte[] Decrypt(
        Stream source,
        ParquetModuleType module,
        short? rowGroupOrdinal = null,
        short? columnOrdinal = null,
        short? pageOrdinal = null) {
        ValidateOrdinals(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        return Algorithm == ParquetEncryptionAlgorithm.AesGcmCtrV1 &&
               module is ParquetModuleType.DataPage or ParquetModuleType.DictionaryPage
            ? DecryptCtr(source)
            : DecryptGcm(source, module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
    }

    public byte[] SignFooter(ReadOnlySpan<byte> footerBytes) {
        CountGcmInvocation();
        byte[] nonce = RandomNumberGenerator.GetBytes(NonceLength);
        byte[] ciphertext = new byte[footerBytes.Length];
        byte[] tag = new byte[TagLength];
        byte[] aad = BuildAad(ParquetModuleType.Footer);
        using(var gcm = new AesGcm(_key, TagLength)) {
            gcm.Encrypt(nonce, footerBytes, ciphertext, tag, aad);
        }

        byte[] signature = new byte[NonceLength + TagLength];
        nonce.CopyTo(signature, 0);
        tag.CopyTo(signature, NonceLength);
        return signature;
    }

    public void VerifyFooter(ReadOnlySpan<byte> footerBytes, ReadOnlySpan<byte> signature) {
        if(signature.Length != NonceLength + TagLength)
            throw new InvalidDataException("A plaintext footer signature must contain 28 bytes.");

        ReadOnlySpan<byte> nonce = signature[..NonceLength];
        ReadOnlySpan<byte> expectedTag = signature[NonceLength..];
        byte[] ciphertext = new byte[footerBytes.Length];
        byte[] actualTag = new byte[TagLength];
        byte[] aad = BuildAad(ParquetModuleType.Footer);
        using(var gcm = new AesGcm(_key, TagLength)) {
            gcm.Encrypt(nonce, footerBytes, ciphertext, actualTag, aad);
        }

        if(!CryptographicOperations.FixedTimeEquals(expectedTag, actualTag))
            throw new CryptographicException("The plaintext footer signature is invalid.");
    }

    public byte[] BuildAad(
        ParquetModuleType module,
        short? rowGroupOrdinal = null,
        short? columnOrdinal = null,
        short? pageOrdinal = null) {
        ValidateOrdinals(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        int suffixLength = 1 + (module == ParquetModuleType.Footer ? 0 : 4) +
                           (module is ParquetModuleType.DataPage or ParquetModuleType.DataPageHeader ? 2 : 0);
        byte[] aad = new byte[_aadPrefix.Length + _aadFileUnique.Length + suffixLength];
        int offset = 0;
        _aadPrefix.CopyTo(aad, offset);
        offset += _aadPrefix.Length;
        _aadFileUnique.CopyTo(aad, offset);
        offset += _aadFileUnique.Length;
        aad[offset++] = (byte)module;

        if(module != ParquetModuleType.Footer) {
            BinaryPrimitives.WriteInt16LittleEndian(aad.AsSpan(offset, 2), rowGroupOrdinal!.Value);
            offset += 2;
            BinaryPrimitives.WriteInt16LittleEndian(aad.AsSpan(offset, 2), columnOrdinal!.Value);
            offset += 2;
        }
        if(module is ParquetModuleType.DataPage or ParquetModuleType.DataPageHeader)
            BinaryPrimitives.WriteInt16LittleEndian(aad.AsSpan(offset, 2), pageOrdinal!.Value);

        return aad;
    }

    internal static void ValidateTrailingPadding(
        ReadOnlySpan<byte> plaintext,
        long consumed,
        string moduleName) {
        if(consumed < 0 || consumed > plaintext.Length)
            throw new InvalidDataException($"The parsed {moduleName} length is invalid.");

        for(int i = checked((int)consumed); i < plaintext.Length; i++) {
            if(plaintext[i] != 0)
                throw new InvalidDataException($"Unexpected non-zero bytes follow the {moduleName}.");
        }
    }

    private byte[] DecryptGcm(
        Stream source,
        ParquetModuleType module,
        short? rowGroupOrdinal,
        short? columnOrdinal,
        short? pageOrdinal) {
        int payloadLength = ReadPayloadLength(source, NonceLength + TagLength);
        byte[] payload = source.ReadBytesExactly(payloadLength);
        int ciphertextLength = payloadLength - NonceLength - TagLength;
        byte[] plaintext = new byte[ciphertextLength];
        byte[] aad = BuildAad(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        using(var gcm = new AesGcm(_key, TagLength)) {
            gcm.Decrypt(
                payload.AsSpan(0, NonceLength),
                payload.AsSpan(NonceLength, ciphertextLength),
                payload.AsSpan(NonceLength + ciphertextLength, TagLength),
                plaintext,
                aad);
        }
        return plaintext;
    }

    private byte[] EncryptCtr(ReadOnlySpan<byte> plaintext) {
        byte[] nonce = RandomNumberGenerator.GetBytes(NonceLength);
        byte[] ciphertext = TransformCtr(plaintext, nonce);
        return Frame(nonce, ciphertext, ReadOnlySpan<byte>.Empty);
    }

    private byte[] DecryptCtr(Stream source) {
        int payloadLength = ReadPayloadLength(source, NonceLength);
        byte[] payload = source.ReadBytesExactly(payloadLength);
        return TransformCtr(payload.AsSpan(NonceLength), payload.AsSpan(0, NonceLength));
    }

    private byte[] TransformCtr(ReadOnlySpan<byte> input, ReadOnlySpan<byte> nonce) {
        byte[] counter = new byte[16];
        nonce.CopyTo(counter);
        counter[15] = 1;
        byte[] output = new byte[input.Length];
        byte[] keyStream = new byte[16];

        using Aes aes = Aes.Create();
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.None;
        aes.Key = _key;
        using ICryptoTransform encryptor = aes.CreateEncryptor();

        for(int offset = 0; offset < input.Length; offset += 16) {
            encryptor.TransformBlock(counter, 0, counter.Length, keyStream, 0);
            int count = Math.Min(16, input.Length - offset);
            for(int i = 0; i < count; i++)
                output[offset + i] = (byte)(input[offset + i] ^ keyStream[i]);

            if(!IncrementCounter(counter))
                throw new CryptographicException("AES-CTR counter overflowed.");
        }
        return output;
    }

    private static bool IncrementCounter(Span<byte> counter) {
        for(int i = 15; i >= 12; i--) {
            counter[i]++;
            if(counter[i] != 0)
                return true;
        }
        return false;
    }

    private static byte[] Frame(ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> tag) {
        int payloadLength = checked(nonce.Length + ciphertext.Length + tag.Length);
        byte[] framed = new byte[checked(sizeof(int) + payloadLength)];
        BinaryPrimitives.WriteInt32LittleEndian(framed, payloadLength);
        nonce.CopyTo(framed.AsSpan(sizeof(int)));
        ciphertext.CopyTo(framed.AsSpan(sizeof(int) + nonce.Length));
        tag.CopyTo(framed.AsSpan(sizeof(int) + nonce.Length + ciphertext.Length));
        return framed;
    }

    private static int ReadPayloadLength(Stream source, int minimumLength) {
        Span<byte> lengthBytes = stackalloc byte[sizeof(int)];
        int totalRead = 0;
        while(totalRead < lengthBytes.Length) {
            int read = source.Read(lengthBytes[totalRead..]);
            if(read == 0)
                break;
            totalRead += read;
        }
        if(totalRead != sizeof(int))
            throw new InvalidDataException("The encrypted module length is truncated.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
        if(length < minimumLength)
            throw new InvalidDataException($"The encrypted module length {length} is invalid.");
        if(source.CanSeek && length > source.Length - source.Position)
            throw new InvalidDataException("The encrypted module extends beyond the available data.");
        return length;
    }

    private static void ValidateKey(ReadOnlySpan<byte> key) {
        if(key.Length is not (16 or 24 or 32))
            throw new ArgumentException("AES keys must contain 16, 24, or 32 bytes.", nameof(key));
    }

    private static void ValidateOrdinals(
        ParquetModuleType module,
        short? rowGroupOrdinal,
        short? columnOrdinal,
        short? pageOrdinal) {
        if(module == ParquetModuleType.Footer) {
            if(rowGroupOrdinal != null || columnOrdinal != null || pageOrdinal != null)
                throw new ArgumentException("Footer AAD does not contain ordinals.");
            return;
        }
        if(rowGroupOrdinal is null || rowGroupOrdinal < 0)
            throw new ArgumentOutOfRangeException(nameof(rowGroupOrdinal));
        if(columnOrdinal is null || columnOrdinal < 0)
            throw new ArgumentOutOfRangeException(nameof(columnOrdinal));

        bool requiresPage = module is ParquetModuleType.DataPage or ParquetModuleType.DataPageHeader;
        if(requiresPage && (pageOrdinal is null || pageOrdinal < 0))
            throw new ArgumentOutOfRangeException(nameof(pageOrdinal));
        if(!requiresPage && pageOrdinal != null)
            throw new ArgumentException("This module does not contain a page ordinal.", nameof(pageOrdinal));
    }

    private void CountGcmInvocation() {
        long count = Interlocked.Increment(ref _gcmInvocationCount);
        if(count > uint.MaxValue)
            throw new CryptographicException("The AES-GCM invocation limit for this key has been exceeded.");
    }
}
