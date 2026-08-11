using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Parquet.Encryption;
using Xunit;

namespace Parquet.Test.Encryption;

public class ParquetCryptoContextTest {
    private static readonly byte[] Key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
    private static readonly byte[] Prefix = "orders/2026-08-11"u8.ToArray();
    private static readonly byte[] FileUnique = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    public void AesGcmRoundTripsEveryModule(int moduleId) {
        ParquetModuleType module = (ParquetModuleType)moduleId;
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        byte[] plaintext = Enumerable.Range(0, 97).Select(i => (byte)i).ToArray();

        byte[] encrypted = Encrypt(crypto, plaintext, module);
        using var stream = new MemoryStream(encrypted, writable: false);
        byte[] decrypted = Decrypt(crypto, stream, module);

        Assert.Equal(plaintext, decrypted);
        Assert.Equal(stream.Length, stream.Position);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    public void SupportsEveryAesKeySize(int keyLength) {
        byte[] key = Enumerable.Range(0, keyLength).Select(i => (byte)i).ToArray();
        byte[] plaintext = "key-size-round-trip"u8.ToArray();

        foreach(ParquetEncryptionAlgorithm algorithm in Enum.GetValues<ParquetEncryptionAlgorithm>()) {
            var crypto = new ParquetCryptoContext(key, Prefix, FileUnique, algorithm);
            byte[] encrypted = crypto.Encrypt(plaintext, ParquetModuleType.DataPage, 1, 2, 3);
            using var stream = new MemoryStream(encrypted, writable: false);
            Assert.Equal(plaintext, crypto.Decrypt(stream, ParquetModuleType.DataPage, 1, 2, 3));
        }
    }

    [Fact]
    public void GcmFrameCanBeDecryptedIndependently() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        byte[] plaintext = "independent-gcm-check"u8.ToArray();
        byte[] framed = crypto.Encrypt(plaintext, ParquetModuleType.DataPageHeader, 2, 4, 6);
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(framed);
        ReadOnlySpan<byte> nonce = framed.AsSpan(4, 12);
        ReadOnlySpan<byte> ciphertext = framed.AsSpan(16, payloadLength - 28);
        ReadOnlySpan<byte> tag = framed.AsSpan(4 + payloadLength - 16, 16);
        byte[] actual = new byte[ciphertext.Length];

        using(var gcm = new AesGcm(Key, 16)) {
            gcm.Decrypt(
                nonce,
                ciphertext,
                tag,
                actual,
                crypto.BuildAad(ParquetModuleType.DataPageHeader, 2, 4, 6));
        }

        Assert.Equal(plaintext, actual);
        Assert.Equal(framed.Length - 4, payloadLength);
    }

    [Fact]
    public void CtrPageFrameUsesNonceAndBigEndianCounter() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmCtrV1);
        byte[] plaintext = Enumerable.Range(0, 53).Select(i => (byte)i).ToArray();
        byte[] framed = crypto.Encrypt(plaintext, ParquetModuleType.DataPage, 0, 0, 0);
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(framed);
        ReadOnlySpan<byte> nonce = framed.AsSpan(4, 12);
        ReadOnlySpan<byte> ciphertext = framed.AsSpan(16);

        Assert.Equal(12 + plaintext.Length, payloadLength);
        Assert.Equal(TransformCtr(plaintext, nonce), ciphertext.ToArray());
    }

    [Fact]
    public void CtrAlgorithmStillAuthenticatesHeaders() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmCtrV1);
        byte[] encrypted = crypto.Encrypt(
            "header"u8,
            ParquetModuleType.DataPageHeader,
            0,
            0,
            0);
        encrypted[^1] ^= 0x80;

        using var stream = new MemoryStream(encrypted, writable: false);
        Assert.Throws<AuthenticationTagMismatchException>(() =>
            crypto.Decrypt(stream, ParquetModuleType.DataPageHeader, 0, 0, 0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(4103)]
    [InlineData(262144)]
    public void CtrDataPageRoundTripsBoundarySizes(int size) {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmCtrV1);
        byte[] plaintext = Enumerable.Range(0, size).Select(i => (byte)i).ToArray();
        byte[] encrypted = crypto.Encrypt(plaintext, ParquetModuleType.DataPage, 1, 2, 3);
        using var stream = new MemoryStream(encrypted, writable: false);

        Assert.Equal(
            plaintext,
            crypto.Decrypt(stream, ParquetModuleType.DataPage, 1, 2, 3));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(31)]
    [InlineData(32)]
    public void CtrDictionaryPageRoundTripsBoundarySizes(int size) {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmCtrV1);
        byte[] plaintext = Enumerable.Range(0, size).Select(i => (byte)(i * 7)).ToArray();
        byte[] encrypted = crypto.Encrypt(plaintext, ParquetModuleType.DictionaryPage, 1, 2);
        using var stream = new MemoryStream(encrypted, writable: false);

        Assert.Equal(
            plaintext,
            crypto.Decrypt(stream, ParquetModuleType.DictionaryPage, 1, 2));
    }

    [Fact]
    public void RejectsCtrFramingForPageHeader() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmCtrV1);
        byte[] encrypted = crypto.Encrypt(
            new byte[32],
            ParquetModuleType.DataPage,
            0,
            0,
            0);
        using var stream = new MemoryStream(encrypted, writable: false);

        Assert.Throws<AuthenticationTagMismatchException>(() =>
            crypto.Decrypt(stream, ParquetModuleType.DataPageHeader, 0, 0, 0));
    }

    [Fact]
    public void ModuleAndOrdinalsAreAuthenticated() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        byte[] encrypted = crypto.Encrypt(
            "page-header"u8,
            ParquetModuleType.DataPageHeader,
            3,
            4,
            5);

        AssertAuthenticationFailure(encrypted, crypto, ParquetModuleType.DataPageHeader, 2, 4, 5);
        AssertAuthenticationFailure(encrypted, crypto, ParquetModuleType.DataPageHeader, 3, 2, 5);
        AssertAuthenticationFailure(encrypted, crypto, ParquetModuleType.DataPageHeader, 3, 4, 2);
        AssertAuthenticationFailure(encrypted, crypto, ParquetModuleType.DataPage, 3, 4, 5);
    }

    [Fact]
    public void FileUniquePreventsCrossFileModuleSwap() {
        var first = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        var second = new ParquetCryptoContext(
            Key,
            Prefix,
            new byte[] { 8, 7, 6, 5, 4, 3, 2, 1 },
            ParquetEncryptionAlgorithm.AesGcmV1);
        byte[] encrypted = first.Encrypt(
            "metadata"u8,
            ParquetModuleType.ColumnMetaData,
            0,
            0);

        using var stream = new MemoryStream(encrypted, writable: false);
        Assert.Throws<AuthenticationTagMismatchException>(() =>
            second.Decrypt(stream, ParquetModuleType.ColumnMetaData, 0, 0));
    }

    [Fact]
    public void FooterSignatureAuthenticatesSerializedFooter() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        byte[] footer = "serialized-footer"u8.ToArray();
        byte[] signature = crypto.SignFooter(footer);

        crypto.VerifyFooter(footer, signature);
        footer[0] ^= 0x01;
        Assert.Throws<CryptographicException>(() => crypto.VerifyFooter(footer, signature));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(11)]
    [InlineData(27)]
    public void RejectsShortGcmPayloads(int payloadLength) {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        using MemoryStream stream = FrameWithDeclaredLength(payloadLength, payloadLength);

        Assert.Throws<InvalidDataException>(() =>
            crypto.Decrypt(stream, ParquetModuleType.ColumnIndex, 0, 0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(11)]
    public void RejectsShortCtrPayloads(int payloadLength) {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmCtrV1);
        using MemoryStream stream = FrameWithDeclaredLength(payloadLength, payloadLength);

        Assert.Throws<InvalidDataException>(() =>
            crypto.Decrypt(stream, ParquetModuleType.DictionaryPage, 0, 0));
    }

    [Fact]
    public void RejectsPayloadThatExtendsPastInput() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        using MemoryStream stream = FrameWithDeclaredLength(64, 28);

        Assert.Throws<InvalidDataException>(() =>
            crypto.Decrypt(stream, ParquetModuleType.OffsetIndex, 0, 0));
    }

    [Fact]
    public void ReadsLengthPrefixFromChunkedStream() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);
        byte[] plaintext = "chunked"u8.ToArray();
        byte[] encrypted = crypto.Encrypt(plaintext, ParquetModuleType.BloomFilterHeader, 1, 2);
        using var stream = new ChunkedMemoryStream(encrypted);

        Assert.Equal(plaintext, crypto.Decrypt(stream, ParquetModuleType.BloomFilterHeader, 1, 2));
    }

    [Fact]
    public void RejectsInvalidOrdinalShapes() {
        var crypto = CreateContext(ParquetEncryptionAlgorithm.AesGcmV1);

        Assert.Throws<ArgumentException>(() =>
            crypto.BuildAad(ParquetModuleType.Footer, 0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            crypto.BuildAad(ParquetModuleType.ColumnMetaData));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            crypto.BuildAad(ParquetModuleType.DataPage, 0, 0));
        Assert.Throws<ArgumentException>(() =>
            crypto.BuildAad(ParquetModuleType.DictionaryPage, 0, 0, 0));
    }

    [Fact]
    public void AcceptsZeroPaddingAfterEncryptedThriftModule() {
        ParquetCryptoContext.ValidateTrailingPadding(
            new byte[] { 0x15, 0x00, 0x00, 0x00 },
            consumed: 2,
            "test module");
    }

    [Fact]
    public void RejectsNonZeroPaddingAfterEncryptedThriftModule() {
        Assert.Throws<InvalidDataException>(() =>
            ParquetCryptoContext.ValidateTrailingPadding(
                new byte[] { 0x15, 0x00, 0x00, 0x01 },
                consumed: 2,
                "test module"));
    }

    private static ParquetCryptoContext CreateContext(ParquetEncryptionAlgorithm algorithm) =>
        new(Key, Prefix, FileUnique, algorithm);

    private static byte[] Encrypt(
        ParquetCryptoContext crypto,
        byte[] plaintext,
        ParquetModuleType module) {
        return module switch {
            ParquetModuleType.Footer => crypto.Encrypt(plaintext, module),
            ParquetModuleType.DataPage or ParquetModuleType.DataPageHeader =>
                crypto.Encrypt(plaintext, module, 1, 2, 3),
            _ => crypto.Encrypt(plaintext, module, 1, 2)
        };
    }

    private static byte[] Decrypt(
        ParquetCryptoContext crypto,
        Stream stream,
        ParquetModuleType module) {
        return module switch {
            ParquetModuleType.Footer => crypto.Decrypt(stream, module),
            ParquetModuleType.DataPage or ParquetModuleType.DataPageHeader =>
                crypto.Decrypt(stream, module, 1, 2, 3),
            _ => crypto.Decrypt(stream, module, 1, 2)
        };
    }

    private static void AssertAuthenticationFailure(
        byte[] encrypted,
        ParquetCryptoContext crypto,
        ParquetModuleType module,
        short rowGroup,
        short column,
        short page) {
        using var stream = new MemoryStream(encrypted, writable: false);
        Assert.Throws<AuthenticationTagMismatchException>(() =>
            crypto.Decrypt(stream, module, rowGroup, column, page));
    }

    private static MemoryStream FrameWithDeclaredLength(int declaredLength, int actualLength) {
        byte[] buffer = new byte[4 + actualLength];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, declaredLength);
        return new MemoryStream(buffer, writable: false);
    }

    private static byte[] TransformCtr(ReadOnlySpan<byte> input, ReadOnlySpan<byte> nonce) {
        byte[] counter = new byte[16];
        nonce.CopyTo(counter);
        counter[15] = 1;
        byte[] output = new byte[input.Length];
        byte[] keyStream = new byte[16];
        using Aes aes = Aes.Create();
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.None;
        aes.Key = Key;
        using ICryptoTransform encryptor = aes.CreateEncryptor();

        for(int offset = 0; offset < input.Length; offset += 16) {
            encryptor.TransformBlock(counter, 0, counter.Length, keyStream, 0);
            int count = Math.Min(16, input.Length - offset);
            for(int i = 0; i < count; i++)
                output[offset + i] = (byte)(input[offset + i] ^ keyStream[i]);
            for(int i = 15; i >= 12 && ++counter[i] == 0; i--) { }
        }
        return output;
    }

    private sealed class ChunkedMemoryStream(byte[] buffer) : MemoryStream(buffer, writable: false) {
        public override int Read(Span<byte> destination) =>
            base.Read(destination[..Math.Min(1, destination.Length)]);

        public override int Read(byte[] buffer, int offset, int count) =>
            base.Read(buffer, offset, Math.Min(1, count));
    }
}
