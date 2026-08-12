using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test;

public class ParquetFooterTest {
    private static readonly byte[] Key = "0123456789ABCDEF"u8.ToArray();
    private static readonly byte[] WrongKey = "FEDCBA9876543210"u8.ToArray();
    private static readonly byte[] AadPrefix = "footer-rewrite/tests"u8.ToArray();
    private static readonly byte[] KeyMetadata = "kms/footer"u8.ToArray();

    [Theory]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, false)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, false)]
    public async Task UpdatesAuthenticatedCustomMetadataWithoutChangingRowGroups(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter) {
        using MemoryStream stream = await WriteFileAsync(algorithm, encryptFooter);
        byte[] before = stream.ToArray();
        int originalFooterStart = GetFooterStart(before);

        await ParquetFooter.UpdateCustomMetadataAsync(
            stream,
            new Dictionary<string, string> {
                ["replace"] = "new",
                ["added"] = "value"
            },
            CreateReadOptions(Key));

        byte[] after = stream.ToArray();
        int rewrittenFooterStart = GetFooterStart(after);
        Assert.Equal(originalFooterStart, rewrittenFooterStart);
        Assert.Equal(
            before.AsSpan(0, originalFooterStart).ToArray(),
            after.AsSpan(0, rewrittenFooterStart).ToArray());
        Assert.Equal(encryptFooter ? "PARE" : "PAR1", ReadMagic(after));

        stream.Position = 0;
        await using ParquetReader reader = await ParquetReader.CreateAsync(
            stream,
            CreateReadOptions(Key));
        Assert.Equal("preserved", reader.CustomMetadata["keep"]);
        Assert.Equal("new", reader.CustomMetadata["replace"]);
        Assert.Equal("value", reader.CustomMetadata["added"]);
        Assert.Equal(KeyMetadata, encryptFooter
            ? ReadEncryptedFooterKeyMetadata(after)
            : reader.Metadata!.FooterSigningKeyMetadata);

        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        int[] values = new int[4];
        await rowGroup.ReadAsync<int>(reader.Schema.DataFields[0], values);
        Assert.Equal([3, 5, 8, 13], values);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WrongKeyLeavesAuthenticatedFileUnchanged(bool encryptFooter) {
        using MemoryStream stream = await WriteFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            encryptFooter);
        byte[] before = stream.ToArray();

        await Assert.ThrowsAnyAsync<CryptographicException>(() =>
            ParquetFooter.UpdateCustomMetadataAsync(
                stream,
                new Dictionary<string, string> { ["added"] = "value" },
                CreateReadOptions(WrongKey)));

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task FilePathOverloadUpdatesPlaintextFooter() {
        string filePath = Path.GetTempFileName();
        try {
            var field = new DataField<int>("value");
            await using(Stream output = System.IO.File.Create(filePath)) {
                await using ParquetWriter writer = await ParquetWriter.CreateAsync(
                    new ParquetSchema(field),
                    output);
                writer.CustomMetadata = new Dictionary<string, string> { ["keep"] = "original" };
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteAsync<int>(field, new int[] { 21, 34 });
            }

            await ParquetFooter.UpdateCustomMetadataAsync(
                filePath,
                new Dictionary<string, string> { ["added"] = "value" });

            await using ParquetReader reader = await ParquetReader.CreateAsync(filePath);
            Assert.Equal("original", reader.CustomMetadata["keep"]);
            Assert.Equal("value", reader.CustomMetadata["added"]);
        } finally {
            System.IO.File.Delete(filePath);
        }
    }

    private static async Task<MemoryStream> WriteFileAsync(
        ParquetEncryptionAlgorithm algorithm,
        bool encryptFooter) {
        var field = new DataField<int>("value");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(Key, KeyMetadata)) {
            Algorithm = algorithm,
            EncryptFooter = encryptFooter,
            AadPrefix = AadPrefix,
            StoreAadPrefix = false
        };
        var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field),
                        stream,
                        new ParquetOptions { Encryption = encryption })) {
            writer.CustomMetadata = new Dictionary<string, string> {
                ["keep"] = "preserved",
                ["replace"] = "old"
            };
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int[] { 3, 5, 8, 13 });
        }
        return stream;
    }

    private static ParquetOptions CreateReadOptions(byte[] key) =>
        new() {
            Decryption = new ParquetDecryptionOptions {
                FooterKey = key,
                AadPrefix = AadPrefix
            }
        };

    private static int GetFooterStart(byte[] file) {
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 8, 4));
        return checked(file.Length - 8 - footerLength);
    }

    private static string ReadMagic(byte[] file) =>
        Encoding.ASCII.GetString(file, file.Length - 4, 4);

    private static byte[]? ReadEncryptedFooterKeyMetadata(byte[] file) {
        int footerStart = GetFooterStart(file);
        using var stream = new MemoryStream(file, footerStart, file.Length - footerStart - 8);
        return Meta.FileCryptoMetaData
            .Read(new Meta.Proto.ThriftCompactProtocolReader(stream))
            .KeyMetadata;
    }
}
