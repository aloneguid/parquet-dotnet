using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption;

public class ParquetEncryptionValidationTest {
    private static readonly byte[] Key = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();

    [Theory]
    [InlineData(0)]
    [InlineData(15)]
    [InlineData(17)]
    [InlineData(23)]
    [InlineData(25)]
    [InlineData(31)]
    [InlineData(33)]
    public void RejectsInvalidAesKeyLengths(int keyLength) {
        Assert.Throws<ArgumentException>(() => new ParquetKey(new byte[keyLength]));
    }

    [Fact]
    public async Task RequiresFooterKeyForEncryptedFooter() {
        using MemoryStream stream = await WriteFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        stream.Position = 0;

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ParquetReader.CreateAsync(stream));
    }

    [Fact]
    public async Task RequiresFooterKeyForSignedPlaintextFooter() {
        using MemoryStream stream = await WriteFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: false);
        stream.Position = 0;

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ParquetReader.CreateAsync(stream));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    public async Task RejectsInvalidFooterLength(int footerLength) {
        using MemoryStream original = await WriteFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        byte[] bytes = original.ToArray();
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4), footerLength);
        using var tampered = new MemoryStream(bytes, writable: false);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ParquetReader.CreateAsync(tampered, CreateReadOptions()));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RejectsMismatchedMagic(bool tamperHead) {
        using MemoryStream original = await WriteFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        byte[] bytes = original.ToArray();
        int offset = tamperHead ? 0 : bytes.Length - 4;
        "PAXX"u8.CopyTo(bytes.AsSpan(offset, 4));
        using var tampered = new MemoryStream(bytes, writable: false);

        await Assert.ThrowsAsync<IOException>(() =>
            ParquetReader.CreateAsync(tampered, CreateReadOptions()));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RejectsTamperedFooterAuthentication(bool encryptFooter) {
        using MemoryStream original = await WriteFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter);
        byte[] bytes = original.ToArray();
        bytes[^9] ^= 0x20;
        using var tampered = new MemoryStream(bytes, writable: false);

        await Assert.ThrowsAnyAsync<CryptographicException>(() =>
            ParquetReader.CreateAsync(tampered, CreateReadOptions()));
    }

    [Fact]
    public async Task RejectsWrongExternalAadPrefix() {
        var field = new DataField<int>("id");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(Key)) {
            AadPrefix = "correct-prefix"u8.ToArray(),
            StoreAadPrefix = false
        };
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field),
                        stream,
                        new ParquetOptions { Encryption = encryption })) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int[] { 1 });
        }
        stream.Position = 0;
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions {
                FooterKey = Key,
                AadPrefix = "wrong-prefix"u8.ToArray()
            }
        };

        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(() =>
            ParquetReader.CreateAsync(stream, options));
    }

    [Fact]
    public async Task RequiresAadWhenStorageIsDisabled() {
        var field = new DataField<int>("id");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(Key)) {
            StoreAadPrefix = false
        };

        await Assert.ThrowsAsync<ArgumentException>(() =>
            ParquetWriter.CreateAsync(
                new ParquetSchema(field),
                new MemoryStream(),
                new ParquetOptions { Encryption = encryption }));
    }

    [Fact]
    public async Task RejectsAppendingWithEncryptionOptions() {
        using var stream = new MemoryStream();
        var field = new DataField<int>("id");
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field), stream)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int[] { 1, 2 });
        }
        stream.Position = 0;
        var options = new ParquetOptions {
            Encryption = new ParquetEncryptionOptions(new ParquetKey(Key))
        };

        await Assert.ThrowsAsync<NotSupportedException>(() =>
            ParquetWriter.CreateAsync(
                new ParquetSchema(new DataField<int>("id")),
                stream,
                options,
                append: true));
    }

    [Fact]
    public async Task RejectsAppendingToEncryptedFile() {
        using MemoryStream stream = await WriteFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        stream.Position = 0;

        await Assert.ThrowsAsync<NotSupportedException>(() =>
            ParquetWriter.CreateAsync(
                new ParquetSchema(new DataField<int>("id")),
                stream,
                append: true));
    }

    [Fact]
    public async Task DetectsTamperedEncryptedColumnMetadata() {
        byte[] columnKey = Enumerable.Range(17, 16).Select(i => (byte)i).ToArray();
        var id = new DataField<int>("id");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(Key)) {
            EncryptAllColumns = false
        };
        encryption.ColumnKeys["id"] = new ParquetKey(columnKey);
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(id),
                        stream,
                        new ParquetOptions { Encryption = encryption })) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(id, new int[] { 1, 2, 3 });
        }
        stream.Position = 0;
        var readOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = Key }
        };
        readOptions.Decryption.ColumnKeys["id"] = columnKey;
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, readOptions);
        byte[] encryptedMetadata = reader.Metadata!.RowGroups[0].Columns[0].EncryptedColumnMetadata!;
        encryptedMetadata[^1] ^= 0x01;
        using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
        DataField field = reader.Schema.DataFields[0];

        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(() =>
            rowGroupReader.ReadAsync<int>(field, new int[3]).AsTask());
    }

    [Theory]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, false)]
    public async Task PageTamperingMatchesAlgorithmGuarantees(
        ParquetEncryptionAlgorithm algorithm,
        bool authenticatedBody) {
        using MemoryStream original = await WriteFileAsync(algorithm, encryptFooter: true);
        long pageOffset;
        original.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(original, CreateReadOptions())) {
            pageOffset = reader.Metadata!.RowGroups[0].Columns[0].MetaData!.DataPageOffset;
        }
        byte[] bytes = original.ToArray();
        int headerPayloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            bytes.AsSpan(checked((int)pageOffset), 4));
        int bodyOffset = checked((int)pageOffset + 4 + headerPayloadLength);
        bytes[bodyOffset + 4 + 12] ^= 0x01;
        using var tampered = new MemoryStream(bytes, writable: false);
        await using ParquetReader tamperedReader = await ParquetReader.CreateAsync(
            tampered,
            CreateReadOptions());
        using ParquetRowGroupReader rowGroup = tamperedReader.OpenRowGroupReader(0);
        DataField field = tamperedReader.Schema.DataFields[0];

        if(authenticatedBody) {
            await Assert.ThrowsAsync<AuthenticationTagMismatchException>(() =>
                rowGroup.ReadAsync<int>(field, new int[4]).AsTask());
        } else {
            int[] values = new int[4];
            await rowGroup.ReadAsync<int>(field, values);
            Assert.NotEqual(new int[] { 1, 2, 3, 4 }, values);
        }
    }

    private static async Task<MemoryStream> WriteFileAsync(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter) {
        var field = new DataField<int>("id");
        var options = new ParquetOptions {
            CompressionMethod = CompressionMethod.None,
            Encryption = new ParquetEncryptionOptions(new ParquetKey(Key)) {
                Algorithm = algorithm,
                EncryptFooter = encryptFooter
            }
        };
        var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field), stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int[] { 1, 2, 3, 4 });
        }
        return stream;
    }

    private static ParquetOptions CreateReadOptions() => new() {
        Decryption = new ParquetDecryptionOptions { FooterKey = Key }
    };
}
