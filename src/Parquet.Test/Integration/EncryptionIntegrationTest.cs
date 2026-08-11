using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Integration;

public class EncryptionIntegrationTest : IntegrationBase {
    private static readonly byte[] FooterMetadata = "kms/footer"u8.ToArray();
    private static readonly byte[] ColumnMetadata = "kms/secret"u8.ToArray();

    [Theory]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true, 16, CompressionMethod.None)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, false, 24, CompressionMethod.Snappy)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true, 32, CompressionMethod.Gzip)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, true, 16, CompressionMethod.Snappy)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, false, 24, CompressionMethod.Gzip)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, true, 32, CompressionMethod.None)]
    public async Task RoundTripsMixedColumnsAcrossRowGroups(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter,
        int keyLength,
        CompressionMethod compression) {
        byte[] footerKey = CreateKey(keyLength, 0x10);
        byte[] columnKey = CreateKey(keyLength, 0x40);
        using MemoryStream stream = await WriteMixedFileAsync(
            algorithm,
            encryptFooter,
            compression,
            footerKey,
            columnKey,
            rowGroupCount: 3);
        stream.Position = 0;

        await using ParquetReader reader = await ParquetReader.CreateAsync(
            stream,
            CreateDecryptionOptions(footerKey, columnKey));
        Assert.Equal(3, reader.RowGroupCount);

        for(int groupIndex = 0; groupIndex < reader.RowGroupCount; groupIndex++) {
            DataField id = reader.Schema.DataFields[0];
            DataField secret = reader.Schema.DataFields[1];
            DataField published = reader.Schema.DataFields[2];
            using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(groupIndex);
            int[] ids = new int[64];
            string?[] secrets = new string?[64];
            double[] publishedValues = new double[64];

            await rowGroup.ReadAsync<int>(id, ids);
            await rowGroup.ReadAsync(secret, secrets);
            await rowGroup.ReadAsync<double>(published, publishedValues);

            Assert.NotNull(reader.Metadata!.RowGroups[groupIndex].Columns[1].MetaData!.DictionaryPageOffset);
            Assert.Equal(Enumerable.Range(groupIndex * 64, 64), ids);
            Assert.Equal(
                Enumerable.Range(0, 64).Select(i => (string?)$"group-{groupIndex}-value-{i % 5}"),
                secrets);
            Assert.Equal(
                Enumerable.Range(0, 64).Select(i => groupIndex + i / 10.0),
                publishedValues);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ProtectsColumnMetadataForEachFooterMode(bool encryptFooter) {
        byte[] footerKey = CreateKey(16, 0x10);
        byte[] columnKey = CreateKey(16, 0x40);
        using MemoryStream stream = await WriteMixedFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter,
            CompressionMethod.None,
            footerKey,
            columnKey,
            rowGroupCount: 1);
        stream.Position = 0;

        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = footerKey }
        };
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        ColumnChunk secret = reader.Metadata!.RowGroups[0].Columns[1];

        Assert.NotNull(secret.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY);
        Assert.NotEmpty(secret.EncryptedColumnMetadata!);
        if(encryptFooter) {
            Assert.Null(secret.MetaData);
        } else {
            Assert.NotNull(secret.MetaData);
            Assert.Null(secret.MetaData.Statistics);
        }
    }

    [Fact]
    public async Task ResolvesFooterAndColumnKeysFromTheirMetadata() {
        byte[] footerKey = CreateKey(32, 0x10);
        byte[] columnKey = CreateKey(32, 0x40);
        using MemoryStream stream = await WriteMixedFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true,
            CompressionMethod.None,
            footerKey,
            columnKey,
            rowGroupCount: 1);
        stream.Position = 0;
        var retriever = new RecordingKeyRetriever(new Dictionary<string, byte[]> {
            ["kms/footer"] = footerKey,
            ["kms/secret"] = columnKey
        });
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { KeyRetriever = retriever }
        };

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        DataField secret = reader.Schema.DataFields[1];
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        string?[] values = new string?[64];
        await rowGroup.ReadAsync(secret, values);

        Assert.Equal(["kms/footer", "kms/secret"], retriever.Requests);
        Assert.Equal("group-0-value-0", values[0]);
    }

    [Theory]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, false)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, false)]
    public async Task RoundTripsNonAsciiExternalAad(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter) {
        byte[] key = CreateKey(16, 0x20);
        byte[] aad = "kunden/überblick/東京"u8.ToArray();
        var field = new DataField<int>("id");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(key)) {
            Algorithm = algorithm,
            EncryptFooter = encryptFooter,
            AadPrefix = aad,
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
            await rowGroup.WriteAsync<int>(field, new int[] { 3, 5, 8 });
        }

        stream.Position = 0;
        var readOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions {
                FooterKey = key,
                AadPrefix = aad
            }
        };
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, readOptions);
        DataField readField = reader.Schema.DataFields[0];
        using ParquetRowGroupReader readGroup = reader.OpenRowGroupReader(0);
        int[] values = new int[3];
        await readGroup.ReadAsync<int>(readField, values);
        Assert.Equal([3, 5, 8], values);
    }

    [Fact]
    public async Task WrongColumnKeyDoesNotBlockPlaintextProjection() {
        byte[] footerKey = CreateKey(16, 0x10);
        byte[] columnKey = CreateKey(16, 0x40);
        using MemoryStream stream = await WriteMixedFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter: true,
            CompressionMethod.None,
            footerKey,
            columnKey,
            rowGroupCount: 1);
        stream.Position = 0;
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = footerKey }
        };
        options.Decryption.ColumnKeys["secret"] = new byte[16];

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        DataField published = reader.Schema.DataFields[2];
        DataField secret = reader.Schema.DataFields[1];
        double[] publishedValues = new double[64];

        await rowGroup.ReadAsync<double>(published, publishedValues);
        Assert.Equal(0, publishedValues[0]);
        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(
            async () => await rowGroup.ReadAsync(secret, new string?[64]));
    }

    [Fact]
    public async Task CrossFileHeaderSwapFailsAuthentication() {
        byte[] key = CreateKey(16, 0x20);
        byte[] first = await WriteUniformFileAsync(key);
        byte[] second = await WriteUniformFileAsync(key);
        long firstOffset = await GetDataPageOffsetAsync(first, key);
        long secondOffset = await GetDataPageOffsetAsync(second, key);
        int firstFrameLength = checked(4 + BinaryPrimitives.ReadInt32LittleEndian(
            first.AsSpan(checked((int)firstOffset), 4)));
        int secondFrameLength = checked(4 + BinaryPrimitives.ReadInt32LittleEndian(
            second.AsSpan(checked((int)secondOffset), 4)));
        Assert.Equal(firstFrameLength, secondFrameLength);
        Buffer.BlockCopy(
            second,
            checked((int)secondOffset),
            first,
            checked((int)firstOffset),
            firstFrameLength);

        using var tampered = new MemoryStream(first, writable: false);
        await using ParquetReader reader = await ParquetReader.CreateAsync(
            tampered,
            new ParquetOptions {
                Decryption = new ParquetDecryptionOptions { FooterKey = key }
            });
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        DataField field = reader.Schema.DataFields[0];
        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(
            async () => await rowGroup.ReadAsync<int>(field, new int[128]));
    }

    [Fact]
    public async Task ReadsAvailableColumnKeyAndRejectsMissingSecondKey() {
        byte[] footerKey = CreateKey(16, 0x10);
        byte[] salaryKey = CreateKey(16, 0x30);
        byte[] ssnKey = CreateKey(16, 0x50);
        var name = new DataField<string>("name");
        var salary = new DataField<double>("salary");
        var ssn = new DataField<string>("ssn");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(footerKey)) {
            EncryptAllColumns = false
        };
        encryption.ColumnKeys["salary"] = new ParquetKey(salaryKey, "kms/salary"u8);
        encryption.ColumnKeys["ssn"] = new ParquetKey(ssnKey, "kms/ssn"u8);
        var writeOptions = new ParquetOptions { Encryption = encryption };
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(name, salary, ssn), stream, writeOptions)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync(name, new string?[] { "alice", "bob" });
            await rowGroup.WriteAsync<double>(salary, new double[] { 10, 20 });
            await rowGroup.WriteAsync(ssn, new string?[] { "111", "222" });
        }

        stream.Position = 0;
        var partialOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = footerKey }
        };
        partialOptions.Decryption.ColumnKeys["salary"] = salaryKey;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(stream, partialOptions)) {
            using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
            string?[] names = new string?[2];
            double[] salaries = new double[2];
            await rowGroup.ReadAsync(name, names);
            await rowGroup.ReadAsync<double>(salary, salaries);
            Assert.Equal(new string?[] { "alice", "bob" }, names);
            Assert.Equal(new double[] { 10, 20 }, salaries);
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await rowGroup.ReadAsync(ssn, new string?[2]));
        }

        stream.Position = 0;
        var completeOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = footerKey }
        };
        completeOptions.Decryption.ColumnKeys["salary"] = salaryKey;
        completeOptions.Decryption.ColumnKeys["ssn"] = ssnKey;
        await using ParquetReader completeReader = await ParquetReader.CreateAsync(stream, completeOptions);
        using ParquetRowGroupReader completeGroup = completeReader.OpenRowGroupReader(0);
        string?[] ssns = new string?[2];
        await completeGroup.ReadAsync(ssn, ssns);
        Assert.Equal(new string?[] { "111", "222" }, ssns);
    }

    [Theory]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, false)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, false)]
    public async Task StoredAadPrefixTakesPrecedenceOverReaderValue(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter) {
        byte[] key = CreateKey(16, 0x20);
        var field = new DataField<int>("id");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(key)) {
            Algorithm = algorithm,
            EncryptFooter = encryptFooter,
            AadPrefix = "stored-prefix"u8.ToArray(),
            StoreAadPrefix = true
        };
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field),
                        stream,
                        new ParquetOptions { Encryption = encryption })) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int[] { 1, 2, 3 });
        }

        stream.Position = 0;
        var readOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions {
                FooterKey = key,
                AadPrefix = "wrong-reader-prefix"u8.ToArray()
            }
        };
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, readOptions);
        using ParquetRowGroupReader readGroup = reader.OpenRowGroupReader(0);
        int[] values = new int[3];
        await readGroup.ReadAsync<int>(reader.Schema.DataFields[0], values);
        Assert.Equal(new int[] { 1, 2, 3 }, values);
    }

    [Fact]
    public async Task ResolvesPlaintextFooterKeyFromSigningMetadata() {
        byte[] key = CreateKey(16, 0x20);
        byte[] metadata = "kms/signing"u8.ToArray();
        var field = new DataField<int>("id");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(key, metadata)) {
            EncryptFooter = false
        };
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field),
                        stream,
                        new ParquetOptions { Encryption = encryption })) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int[] { 7, 8 });
        }

        stream.Position = 0;
        var retriever = new RecordingKeyRetriever(new Dictionary<string, byte[]> {
            ["kms/signing"] = key
        });
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { KeyRetriever = retriever }
        };
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);

        Assert.Equal(metadata, reader.Metadata!.FooterSigningKeyMetadata);
        Assert.Equal(new string[] { "kms/signing" }, retriever.Requests);
        using ParquetRowGroupReader readGroup = reader.OpenRowGroupReader(0);
        int[] values = new int[2];
        await readGroup.ReadAsync<int>(reader.Schema.DataFields[0], values);
        Assert.Equal(new int[] { 7, 8 }, values);
    }

    private static async Task<MemoryStream> WriteMixedFileAsync(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter,
        CompressionMethod compression,
        byte[] footerKey,
        byte[] columnKey,
        int rowGroupCount) {
        var id = new DataField<int>("id");
        var secret = new DataField<string>("secret");
        var published = new DataField<double>("published");
        var schema = new ParquetSchema(id, secret, published);
        var encryption = new ParquetEncryptionOptions(new ParquetKey(footerKey, FooterMetadata)) {
            Algorithm = algorithm,
            EncryptFooter = encryptFooter,
            EncryptAllColumns = false,
            AadPrefix = "integration/orders"u8.ToArray()
        };
        encryption.FooterKeyColumns.Add("id");
        encryption.ColumnKeys["secret"] = new ParquetKey(columnKey, ColumnMetadata);
        var options = new ParquetOptions {
            CompressionMethod = compression,
            Encryption = encryption
        };
        options.ColumnEncodingHints["secret"] = EncodingHint.Dictionary;
        var stream = new MemoryStream();

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream, options)) {
            for(int groupIndex = 0; groupIndex < rowGroupCount; groupIndex++) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteAsync<int>(id, Enumerable.Range(groupIndex * 64, 64).ToArray());
                await rowGroup.WriteAsync(
                    secret,
                    Enumerable.Range(0, 64)
                        .Select(i => (string?)$"group-{groupIndex}-value-{i % 5}")
                        .ToArray());
                await rowGroup.WriteAsync<double>(
                    published,
                    Enumerable.Range(0, 64).Select(i => groupIndex + i / 10.0).ToArray());
            }
        }
        return stream;
    }

    private static ParquetOptions CreateDecryptionOptions(byte[] footerKey, byte[] columnKey) {
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = footerKey }
        };
        options.Decryption.ColumnKeys["secret"] = columnKey;
        return options;
    }

    private static byte[] CreateKey(int length, byte seed) =>
        Enumerable.Range(0, length).Select(i => checked((byte)(seed + i))).ToArray();

    private static async Task<byte[]> WriteUniformFileAsync(byte[] key) {
        var field = new DataField<int>("value");
        var options = new ParquetOptions {
            CompressionMethod = CompressionMethod.None,
            Encryption = new ParquetEncryptionOptions(new ParquetKey(key))
        };
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field), stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, Enumerable.Range(0, 128).ToArray());
        }
        return stream.ToArray();
    }

    private static async Task<long> GetDataPageOffsetAsync(byte[] file, byte[] key) {
        using var stream = new MemoryStream(file, writable: false);
        await using ParquetReader reader = await ParquetReader.CreateAsync(
            stream,
            new ParquetOptions {
                Decryption = new ParquetDecryptionOptions { FooterKey = key }
            });
        return reader.Metadata!.RowGroups[0].Columns[0].MetaData!.DataPageOffset;
    }

    private sealed class RecordingKeyRetriever(IReadOnlyDictionary<string, byte[]> keys) : IParquetKeyRetriever {
        public List<string> Requests { get; } = new();

        public ReadOnlyMemory<byte> GetKey(ReadOnlyMemory<byte> keyMetadata) {
            string keyId = System.Text.Encoding.UTF8.GetString(keyMetadata.Span);
            Requests.Add(keyId);
            return keys[keyId];
        }
    }
}
