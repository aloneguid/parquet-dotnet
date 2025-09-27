using System;
using System.IO;
using System.Threading.Tasks;
using Interop.Inspector;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Integration {
    public class EncryptedIntegrationTests : IntegrationBase {
        // Adjust if you prefer to hardcode; otherwise leave null to read ENV var.
        // ENV: ENCRYPTED_PARQUET_INSPECTOR_JAR
        private readonly EncParquetInspectorClient _client;

        private const string KeyUtf8 = "footerKey-16byte"; // 16-byte UTF-8 key
        private const string AadPrefix = "mr-suite";

        public EncryptedIntegrationTests() {
            _client = this.CreateInspectorClient();
        }

        private static async Task<string> WriteEncryptedAsync(
            string fileName,
            bool useCtrVariant,
            bool supplyAadPrefix   // true => NOT stored in file; false => stored in file
        ) {
            string outDir = Path.Combine(Environment.CurrentDirectory, "interop-artifacts");
            Directory.CreateDirectory(outDir);
            string path = Path.Combine(outDir, fileName);

            var schema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("name")
            );

            var opts = new ParquetOptions {
                SecretKey = KeyUtf8,
                AADPrefix = AadPrefix,
                SupplyAadPrefix = supplyAadPrefix,
                UseCtrVariant = useCtrVariant
            };

            using(FileStream fs = System.IO.File.Create(path))
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs, formatOptions: opts)) {
                // Keep uncompressed for predictable layout (optional)
                writer.CompressionMethod = CompressionMethod.None;

                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3, 4 }));
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], new[] { "alice", "bob", "carol", "dave" }));
            }

            Assert.True(System.IO.File.Exists(path), "Parquet file was not written.");
            return path;
        }

        // ------------------------
        //         TESTS
        // ------------------------

        [Fact]
        public async Task Gcm_PrefixStored_Readable() {
            string path = await WriteEncryptedAsync(
                fileName: "gcm_prefix_stored.parquet",
                useCtrVariant: false,
                supplyAadPrefix: false // stored in file
            );

            ParquetInspectJson info = await _client.InspectAsync(path, KeyUtf8);

            Assert.Equal("AES_GCM_V1", info.Encryption?.Algorithm);
            Assert.False(info.Encryption?.SupplyAadPrefix ?? true);   // stored
            Assert.False(info.Encryption?.AadSuppliedAtRead ?? true); // we didn't supply

            Assert.Equal(4, info.Totals?.RowCount);
            Assert.True((info.Totals?.RowGroups ?? 0) >= 1);

            Assert.Contains(info.Schema!.Fields!, f => f.Name == "id");
            Assert.Contains(info.Schema!.Fields!, f => f.Name == "name");
        }

        [Fact]
        public async Task Gcm_PrefixSupplied_Readable() {
            string path = await WriteEncryptedAsync(
                fileName: "gcm_prefix_supplied.parquet",
                useCtrVariant: false,
                supplyAadPrefix: true // NOT stored in file
            );

            ParquetInspectJson info = await _client.InspectAsync(path, KeyUtf8, AadPrefix);

            Assert.Equal("AES_GCM_V1", info.Encryption?.Algorithm);
            Assert.True(info.Encryption?.SupplyAadPrefix ?? false);   // not stored
            Assert.True(info.Encryption?.AadSuppliedAtRead ?? false); // we supplied

            Assert.Equal(4, info.Totals?.RowCount);
            Assert.True((info.Totals?.RowGroups ?? 0) >= 1);
        }

        [Fact]
        public async Task Ctr_PrefixStored_Readable() {
            string path = await WriteEncryptedAsync(
                fileName: "ctr_prefix_stored.parquet",
                useCtrVariant: true,
                supplyAadPrefix: false // stored in file
            );

            ParquetInspectJson info = await _client.InspectAsync(path, KeyUtf8);

            Assert.Equal("AES_GCM_CTR_V1", info.Encryption?.Algorithm);
            Assert.False(info.Encryption?.SupplyAadPrefix ?? true);
            Assert.False(info.Encryption?.AadSuppliedAtRead ?? true);

            Assert.Equal(4, info.Totals?.RowCount);
            Assert.True((info.Totals?.RowGroups ?? 0) >= 1);
        }

        [Fact]
        public async Task Ctr_PrefixSupplied_Readable() {
            string path = await WriteEncryptedAsync(
                fileName: "ctr_prefix_supplied.parquet",
                useCtrVariant: true,
                supplyAadPrefix: true // NOT stored in file
            );

            ParquetInspectJson info = await _client.InspectAsync(path, KeyUtf8, AadPrefix);

            Assert.Equal("AES_GCM_CTR_V1", info.Encryption?.Algorithm);
            Assert.True(info.Encryption?.SupplyAadPrefix ?? false);
            Assert.True(info.Encryption?.AadSuppliedAtRead ?? false);

            Assert.Equal(4, info.Totals?.RowCount);
            Assert.True((info.Totals?.RowGroups ?? 0) >= 1);
        }

        [Fact]
        public async Task MissingAad_WhenRequired_YieldsInspectorError() {
            string path = await WriteEncryptedAsync(
                fileName: "gcm_prefix_required_but_missing.parquet",
                useCtrVariant: false,
                supplyAadPrefix: true // NOT stored → must supply to reader
            );

            // We intentionally do NOT pass AAD to the inspector; expect a structured failure.
            EncParquetInspectorException ex = await Assert.ThrowsAsync<EncParquetInspectorException>(
                () => _client.InspectAsync(path, KeyUtf8, aadPrefix: null));

            // Optional: assert message contains Parquet’s AAD complaint
            Assert.Contains("AAD", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
