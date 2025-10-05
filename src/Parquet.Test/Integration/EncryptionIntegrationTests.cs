using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Interop.Inspector;
using Parquet.Data;
using Parquet.Encryption;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Integration {
    public class EncryptionIntegrationTests : IntegrationBase {
        // Adjust if you prefer to hardcode; otherwise leave null to read ENV var.
        // ENV: ENCRYPTED_PARQUET_INSPECTOR_JAR
        private readonly EncParquetInspectorClient _client;

        private const string KeyUtf8 = "footerKey-16byte"; // 16-byte UTF-8 key
        private const string AadPrefix = "mr-suite";

        public EncryptionIntegrationTests() {
            _client = new EncParquetInspectorClient();
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
                FooterEncryptionKey = KeyUtf8,
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
            Assert.False(info.Encryption?.SupplyAadPrefixFlag ?? true);   // stored
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
            Assert.True(info.Encryption?.SupplyAadPrefixFlag ?? false);   // not stored
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
            Assert.False(info.Encryption?.SupplyAadPrefixFlag ?? true);
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
            Assert.True(info.Encryption?.SupplyAadPrefixFlag ?? false);
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

            // Optional: assert message contains "IllegalArgumentException"
            Assert.Contains("IllegalArgumentException", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task PF_Signed_StoredPrefix_Opens_Without_AAD() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = "01234567891234SK", // 16 bytes
                AADPrefix = "pf-demo",
                SupplyAadPrefix = false
            };

            byte[] file;
            using var ms = new MemoryStream();
            using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts))
            using(ParquetRowGroupWriter rg = w.CreateRowGroup())
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3 }));
            file = ms.ToArray();

            using ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(file, false),
                new ParquetOptions { FooterSigningKey = opts.FooterSigningKey });
            Assert.Equal(1, r.RowGroupCount);
        }

        [Fact]
        public async Task PF_Signed_SupplyPrefix_Requires_Prefix_And_Works_When_Given() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = "01234567891234SK",
                AADPrefix = "pf-need-prefix",
                SupplyAadPrefix = true
            };

            byte[] file;
            using var ms = new MemoryStream();
            using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts))
            using(ParquetRowGroupWriter rg = w.CreateRowGroup())
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3 }));
            file = ms.ToArray();

            // Missing prefix → fail early with clear message
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await ParquetReader.CreateAsync(new MemoryStream(file, false),
                    new ParquetOptions { FooterSigningKey = opts.FooterSigningKey }));

            // Correct prefix → succeeds
            using ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(file, false),
                new ParquetOptions {
                    FooterSigningKey = opts.FooterSigningKey,
                    AADPrefix = opts.AADPrefix
                });
            Assert.Equal(1, r.RowGroupCount);
        }

        [Fact]
        public async Task PF_ColumnKeys_Redact_Plaintext_Stats_And_CM_Decrypts() {
            var schema = new ParquetSchema(
                new DataField<string>("name"),
                new DataField<double>("salary")
            );

            const string SignKey = "01234567891234SK";
            const string SalKey = "0123456789123SaK";

            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = SignKey
            };
            opts.ColumnKeys["salary"] = new ParquetOptions.ColumnKeySpec(SalKey, System.Text.Encoding.UTF8.GetBytes("kmeta:salary"));

            byte[] bytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                    using(ParquetRowGroupWriter rg = w.CreateRowGroup()) {
                        await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { "a", "b", "c" }));
                        await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], new[] { 10.0, 20.0, 30.0 }));
                    }
                }
                bytes = ms.ToArray();
            }

            string tailAscii = System.Text.Encoding.ASCII.GetString(bytes, bytes.Length - 4, 4);
            Assert.Equal("PAR1", tailAscii);

            using ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(bytes, false),
                new ParquetOptions { FooterSigningKey = SignKey });
            FileMetaData meta = r.Metadata!;
            ColumnChunk ccSalary = meta.RowGroups[0].Columns[1];

            // Redacted stats in PF
            Assert.NotNull(ccSalary.MetaData!.Statistics);
            Assert.Null(ccSalary.MetaData!.Statistics.MinValue);
            Assert.Null(ccSalary.MetaData!.Statistics.MaxValue);
            Assert.True((ccSalary.EncryptedColumnMetadata?.Length ?? 0) > 0);

            // CM decrypt roundtrip with column key (wrap with 4-byte LE length if needed)
            static MemoryStream WrapLE(byte[] payload) {
                if(payload.Length >= 4) {
                    int len = BitConverter.ToInt32(payload, 0);
                    if(len == payload.Length - 4 && len >= 28) // 12+tag at least
                        return new MemoryStream(payload, writable: false);
                }
                var ms = new MemoryStream(4 + payload.Length);
                byte[] le = BitConverter.GetBytes(payload.Length);
                ms.Write(le, 0, 4);
                ms.Write(payload, 0, payload.Length);
                ms.Position = 0;
                return ms;
            }

            EncryptionBase decr = meta.Decrypter!;
            decr.FooterEncryptionKey = Parquet.Encryption.EncryptionBase.ParseKeyString(SalKey);
            using MemoryStream cmStream = WrapLE(ccSalary.EncryptedColumnMetadata!);
            var rdrProto = new Parquet.Meta.Proto.ThriftCompactProtocolReader(cmStream);
            byte[] plain = decr.DecryptColumnMetaData(rdrProto, meta.RowGroups[0].Ordinal!.Value, (short)1);
            using var msCmd = new MemoryStream(plain, false);
            var cmd = Parquet.Meta.ColumnMetaData.Read(new Parquet.Meta.Proto.ThriftCompactProtocolReader(msCmd));
            Assert.NotNull(cmd.Statistics?.MinValue);
            Assert.NotNull(cmd.Statistics?.MaxValue);
        }

        [Fact]
        public async Task EF_ColumnKeys_Read_Requires_KeyResolver() {
            var schema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("secret")
            );

            const string FooterKey = "01234567891234FK";
            const string ColKey = "01234567891234CK";

            var opts = new ParquetOptions { FooterEncryptionKey = FooterKey };
            opts.ColumnKeys["secret"] = new ParquetOptions.ColumnKeySpec(ColKey);

            byte[] file;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                    using(ParquetRowGroupWriter rg = w.CreateRowGroup()) {
                        await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3 }));
                        await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], new[] { "x", "y", "z" }));
                    }
                }
                file = ms.ToArray();
            }

            string tailAscii = System.Text.Encoding.ASCII.GetString(file, file.Length - 4, 4);
            Assert.Equal("PARE", tailAscii);

            // No resolver → plain column reads, encrypted column fails
            using(ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(file, false),
                       new ParquetOptions { FooterEncryptionKey = FooterKey }))
            using(ParquetRowGroupReader rg = r.OpenRowGroupReader(0)) {
                var id = (DataField)r.Schema.Fields[0];
                var secret = (DataField)r.Schema.Fields[1];
                _ = await rg.ReadColumnAsync(id);
                await Assert.ThrowsAnyAsync<Exception>(async () => await rg.ReadColumnAsync(secret));
            }

            // With resolver → success
            using(ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(file, false),
                       new ParquetOptions {
                           FooterEncryptionKey = FooterKey,
                           ColumnKeyResolver = (path, km) => string.Join(".", path) == "secret" ? ColKey : null
                       }))
            using(ParquetRowGroupReader rg = r.OpenRowGroupReader(0)) {
                var secret = (DataField)r.Schema.Fields[1];
                _ = await rg.ReadColumnAsync(secret);
            }
        }

        [Fact]
        public async Task EF_MultiRowGroup_And_MultiPage_Sanity() {
            var schema = new ParquetSchema(new DataField<int>("v"));
            const string FooterKey = "ef-footer-key-16";

            var opts = new ParquetOptions { FooterEncryptionKey = FooterKey };

            byte[] file;
            using var ms = new MemoryStream();
            using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                // Force small pages by disabling compression and writing enough rows
                w.CompressionMethod = CompressionMethod.None;

                // RG 0
                using(ParquetRowGroupWriter rg = w.CreateRowGroup())
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], Enumerable.Range(0, 10_000).ToArray()));
                // RG 1
                using(ParquetRowGroupWriter rg = w.CreateRowGroup())
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], Enumerable.Range(10_000, 10_000).ToArray()));
            }
            file = ms.ToArray();

            using ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(file, false),
                new ParquetOptions { FooterEncryptionKey = FooterKey });
            Assert.Equal(2, r.RowGroupCount);
            // Just open both groups to ensure decrypt path tolerates multiple ordinals
            using ParquetRowGroupReader rg0 = r.OpenRowGroupReader(0);
            using ParquetRowGroupReader rg1 = r.OpenRowGroupReader(1);
            _ = await rg0.ReadColumnAsync((DataField)r.Schema.Fields[0]);
            _ = await rg1.ReadColumnAsync((DataField)r.Schema.Fields[0]);
        }

        [Fact]
        public async Task EF_Tamper_DataPage_Throws_TagMismatch() {
            var schema = new ParquetSchema(new DataField<int>("v"));
            const string FooterKey = "ef-footer-key-16";
            var opts = new ParquetOptions { FooterEncryptionKey = FooterKey };

            byte[] file;
            using(var ms = new MemoryStream()) {
                using ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts);
                using ParquetRowGroupWriter rg = w.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3, 4, 5 }));
                file = ms.ToArray();
            }

            // Flip one byte somewhere in the middle (very basic tamper)
            byte[] tampered = (byte[])file.Clone();
            for(int i = 100; i < tampered.Length; i++) { tampered[i] ^= 0x01; break; }

            await Assert.ThrowsAnyAsync<Exception>(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(tampered, false),
                    new ParquetOptions { FooterEncryptionKey = FooterKey });
                using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);
                _ = await rg.ReadColumnAsync((DataField)r.Schema.Fields[0]);
            });
        }

        [Fact]
        public async Task EF_Snappy_Compression_Reads() {
            var schema = new ParquetSchema(new DataField<string>("s"));
            const string FooterKey = "ef-footer-key-16";

            byte[] file;
            using var ms = new MemoryStream();
            using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, new ParquetOptions { FooterEncryptionKey = FooterKey })) {
                w.CompressionMethod = CompressionMethod.Snappy;
                using ParquetRowGroupWriter rg = w.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { "a", "b", "c", "d", "e" }));
            }
            file = ms.ToArray();

            using ParquetReader r = await ParquetReader.CreateAsync(new MemoryStream(file, false),
                new ParquetOptions { FooterEncryptionKey = FooterKey });
            using ParquetRowGroupReader rg2 = r.OpenRowGroupReader(0);
            _ = await rg2.ReadColumnAsync((DataField)r.Schema.Fields[0]);
        }
    }
}
