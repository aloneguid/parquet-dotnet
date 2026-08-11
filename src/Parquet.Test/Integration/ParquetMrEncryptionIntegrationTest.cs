using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Interop.Inspector;
using Parquet.Schema;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test.Integration;

public class ParquetMrEncryptionIntegrationTest {
    private const string FooterKeyText = "footerKey-16byte";
    private const string AadPrefixText = "mr-suite";
    private static readonly byte[] FooterKey = Encoding.UTF8.GetBytes(FooterKeyText);
    private static readonly byte[] AadPrefix = Encoding.UTF8.GetBytes(AadPrefixText);

    [Theory]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmV1, false)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, true)]
    [InlineData(ParquetEncryptionAlgorithm.AesGcmCtrV1, false)]
    public async Task ParquetMrReadsEncryptedOutput(
        ParquetEncryptionAlgorithm algorithm,
        bool storeAadPrefix) {
        string path = await WriteEncryptedFileAsync(algorithm, storeAadPrefix);

        try {
            var inspector = new EncParquetInspectorClient();
            ParquetInspectJson info = await inspector.InspectAsync(
                path,
                FooterKey,
                storeAadPrefix ? null : AadPrefix);

            Assert.Equal(
                algorithm == ParquetEncryptionAlgorithm.AesGcmV1
                    ? "AES_GCM_V1"
                    : "AES_GCM_CTR_V1",
                info.Encryption?.Algorithm);
            Assert.Equal(!storeAadPrefix, info.Encryption?.SupplyAadPrefixFlag);
            Assert.Equal(storeAadPrefix, info.Encryption?.HasStoredAadPrefix);
            Assert.Equal(!storeAadPrefix, info.Encryption?.AadSuppliedAtRead);
            Assert.Equal(1, info.Totals?.RowGroups);
            Assert.Equal(4, info.Totals?.RowCount);
            Assert.NotNull(info.Schema?.Fields);
            Assert.Contains(info.Schema.Fields, field => field.Name == "id");
            Assert.Contains(info.Schema.Fields, field => field.Name == "name");
            Assert.All(
                info.RowGroups!.Single().Columns!,
                column => Assert.True(column.EncryptedWithFooterKey));
        } finally {
            F.Delete(path);
        }
    }

    [Fact]
    public async Task ParquetMrRequiresExternalAadPrefix() {
        string path = await WriteEncryptedFileAsync(
            ParquetEncryptionAlgorithm.AesGcmV1,
            storeAadPrefix: false);

        try {
            var inspector = new EncParquetInspectorClient();
            EncParquetInspectorException exception =
                await Assert.ThrowsAsync<EncParquetInspectorException>(() =>
                    inspector.InspectAsync(path, FooterKey));

            Assert.Contains("AAD prefix", exception.Message, StringComparison.OrdinalIgnoreCase);
        } finally {
            F.Delete(path);
        }
    }

    private static async Task<string> WriteEncryptedFileAsync(
        ParquetEncryptionAlgorithm algorithm,
        bool storeAadPrefix) {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"parquet-dotnet-mr-{Guid.NewGuid():N}.parquet");
        var id = new DataField<int>("id");
        var name = new DataField<string>("name");
        var encryption = new ParquetEncryptionOptions(
            new ParquetKey(FooterKey)) {
            Algorithm = algorithm,
            AadPrefix = AadPrefix,
            StoreAadPrefix = storeAadPrefix
        };
        var options = new ParquetOptions {
            CompressionMethod = CompressionMethod.None,
            Encryption = encryption
        };

        await using FileStream stream = F.Create(path);
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(id, name), stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(id, new int[] { 1, 2, 3, 4 });
            await rowGroup.WriteAsync(
                name,
                new string?[] { "alice", "bob", "carol", "dave" });
        }

        return path;
    }
}
