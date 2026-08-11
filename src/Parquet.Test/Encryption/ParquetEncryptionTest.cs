using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Parquet.Encryption;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption;

public class ParquetEncryptionTest {
    private static readonly byte[] FooterKey = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
    private static readonly byte[] ColumnKey = Enumerable.Range(17, 16).Select(i => (byte)i).ToArray();
    private static readonly byte[] FooterKeyMetadata = "footer-key"u8.ToArray();
    private static readonly byte[] ColumnKeyMetadata = "column-key"u8.ToArray();

    [Theory]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, false)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, false)]
    public async Task RoundTripsMixedColumns(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter) {
        using MemoryStream stream = await WriteMixedColumnsAsync(algorithm, encryptFooter);
        byte[] bytes = stream.ToArray();
        byte[] expectedMagic = encryptFooter ? "PARE"u8.ToArray() : "PAR1"u8.ToArray();
        Assert.Equal(expectedMagic, bytes[..4]);
        Assert.Equal(expectedMagic, bytes[^4..]);

        stream.Position = 0;
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, CreateDecryptionOptions());
        Assert.Equal(1, reader.RowGroupCount);

        DataField id = reader.Schema.DataFields[0];
        DataField secret = reader.Schema.DataFields[1];
        DataField published = reader.Schema.DataFields[2];
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);

        int[] ids = new int[4];
        string?[] secrets = new string?[4];
        int[] publishedValues = new int[4];
        await rowGroup.ReadAsync<int>(id, ids);
        await rowGroup.ReadAsync(secret, secrets);
        await rowGroup.ReadAsync<int>(published, publishedValues);

        Assert.Equal([1, 2, 3, 4], ids);
        Assert.Equal(new string?[] { "red", "red", null, "blue" }, secrets);
        Assert.Equal([10, 20, 30, 40], publishedValues);

        var columns = reader.Metadata!.RowGroups[0].Columns;
        Assert.NotNull(columns[0].CryptoMetadata?.ENCRYPTIONWITHFOOTERKEY);
        Assert.Equal(ColumnKeyMetadata, columns[1].CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY?.KeyMetadata);
        Assert.Null(columns[2].CryptoMetadata);
    }

    [Fact]
    public async Task ReadsPlaintextColumnWithoutItsColumnKey() {
        using MemoryStream stream = await WriteMixedColumnsAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        stream.Position = 0;
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = FooterKey }
        };

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        DataField published = reader.Schema.DataFields[2];
        DataField secret = reader.Schema.DataFields[1];
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);

        int[] values = new int[4];
        await rowGroup.ReadAsync<int>(published, values);
        Assert.Equal([10, 20, 30, 40], values);
        await Assert.ThrowsAsync<InvalidDataException>(async () => await rowGroup.ReadAsync(secret, new string?[4]));
    }

    [Fact]
    public async Task ResolvesKeysFromStoredMetadata() {
        using MemoryStream stream = await WriteMixedColumnsAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        stream.Position = 0;
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions {
                KeyRetriever = new TestKeyRetriever()
            }
        };

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        DataField secret = reader.Schema.DataFields[1];
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        string?[] values = new string?[4];
        await rowGroup.ReadAsync(secret, values);

        Assert.Equal(new string?[] { "red", "red", null, "blue" }, values);
    }

    [Fact]
    public async Task RequiresExternalAadPrefixWhenItIsNotStored() {
        var field = new DataField<int>("id");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(FooterKey)) {
            AadPrefix = "dataset/orders"u8.ToArray(),
            StoreAadPrefix = false
        };
        var writeOptions = new ParquetOptions {
            CompressionMethod = CompressionMethod.None,
            Encryption = encryption
        };
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field), stream, writeOptions)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int[] { 1, 2 });
        }

        stream.Position = 0;
        var missingPrefix = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = FooterKey }
        };
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await ParquetReader.CreateAsync(stream, missingPrefix));

        stream.Position = 0;
        var correctPrefix = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions {
                FooterKey = FooterKey,
                AadPrefix = "dataset/orders"u8.ToArray()
            }
        };
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, correctPrefix);
        DataField readField = reader.Schema.DataFields[0];
        using ParquetRowGroupReader readGroup = reader.OpenRowGroupReader(0);
        int[] values = new int[2];
        await readGroup.ReadAsync<int>(readField, values);
        Assert.Equal([1, 2], values);
    }

    [Fact]
    public async Task RejectsWrongFooterKey() {
        using MemoryStream stream = await WriteMixedColumnsAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        stream.Position = 0;
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = new byte[16] }
        };

        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(
            async () => await ParquetReader.CreateAsync(stream, options));
    }

    [Fact]
    public async Task RejectsTamperedPageHeader() {
        using MemoryStream original = await WriteMixedColumnsAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true);
        long dataPageOffset;
        original.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(original, CreateDecryptionOptions())) {
            dataPageOffset = reader.Metadata!.RowGroups[0].Columns[0].MetaData!.DataPageOffset;
        }

        byte[] bytes = original.ToArray();
        bytes[checked((int)dataPageOffset + 8)] ^= 0x40;
        using var tampered = new MemoryStream(bytes);
        await using ParquetReader tamperedReader = await ParquetReader.CreateAsync(tampered, CreateDecryptionOptions());
        DataField id = tamperedReader.Schema.DataFields[0];
        using ParquetRowGroupReader rowGroup = tamperedReader.OpenRowGroupReader(0);

        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(
            async () => await rowGroup.ReadAsync<int>(id, new int[4]));
    }

    [Fact]
    public async Task RejectsTamperedPlaintextFooter() {
        using MemoryStream original = await WriteMixedColumnsAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: false);
        byte[] bytes = original.ToArray();
        bytes[^9] ^= 0x40;
        using var tampered = new MemoryStream(bytes);

        await Assert.ThrowsAsync<CryptographicException>(
            async () => await ParquetReader.CreateAsync(tampered, CreateDecryptionOptions()));
    }

    [Fact]
    public void BuildsModuleAadUsingLittleEndianOrdinals() {
        var crypto = new ParquetCryptoContext(
            FooterKey,
            new byte[] { 0xaa, 0xbb },
            new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 },
            ParquetEncryptionAlgorithm.AesGcmV1);

        byte[] aad = crypto.BuildAad(
            ParquetModuleType.DataPage,
            0x1234,
            0x2345,
            0x3456);

        Assert.Equal(
            new byte[] {
                0xaa, 0xbb,
                1, 2, 3, 4, 5, 6, 7, 8,
                (byte)ParquetModuleType.DataPage,
                0x34, 0x12,
                0x45, 0x23,
                0x56, 0x34
            },
            aad);
    }

    private static async Task<MemoryStream> WriteMixedColumnsAsync(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter) {
        var id = new DataField<int>("id");
        var secret = new DataField<string>("secret");
        var published = new DataField<int>("published");
        var schema = new ParquetSchema(id, secret, published);
        var encryption = new ParquetEncryptionOptions(new ParquetKey(FooterKey, FooterKeyMetadata)) {
            Algorithm = algorithm,
            EncryptFooter = encryptFooter,
            EncryptAllColumns = false,
            AadPrefix = "tenant-a"u8.ToArray()
        };
        encryption.FooterKeyColumns.Add("id");
        encryption.ColumnKeys["secret"] = new ParquetKey(ColumnKey, ColumnKeyMetadata);
        var options = new ParquetOptions {
            CompressionMethod = CompressionMethod.None,
            Encryption = encryption
        };
        options.ColumnEncodingHints["secret"] = EncodingHint.Dictionary;

        var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(id, new int[] { 1, 2, 3, 4 });
            await rowGroup.WriteAsync(secret, new string?[] { "red", "red", null, "blue" });
            await rowGroup.WriteAsync<int>(published, new int[] { 10, 20, 30, 40 });
        }
        return stream;
    }

    private static ParquetOptions CreateDecryptionOptions() {
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = FooterKey }
        };
        options.Decryption.ColumnKeys["secret"] = ColumnKey;
        return options;
    }

    private sealed class TestKeyRetriever : IParquetKeyRetriever {
        private static readonly IReadOnlyDictionary<string, byte[]> Keys = new Dictionary<string, byte[]> {
            ["footer-key"] = FooterKey,
            ["column-key"] = ColumnKey
        };

        public ReadOnlyMemory<byte> GetKey(ReadOnlyMemory<byte> keyMetadata) {
            string name = System.Text.Encoding.UTF8.GetString(keyMetadata.Span);
            return Keys.TryGetValue(name, out byte[]? key)
                ? key
                : throw new InvalidDataException($"Unknown key metadata '{name}'.");
        }
    }
}
