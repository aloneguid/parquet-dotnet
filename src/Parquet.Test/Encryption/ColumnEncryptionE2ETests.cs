using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class ColumnEncryptionE2ETests : TestBase {
        private static ParquetSchema MakeSchema()
            => new ParquetSchema(new DataField<string>("s"));

        [Fact]
        public async Task A_PageEncryption_GcmV1_RoundTrip() {
            ParquetSchema schema = MakeSchema();
            var opts = new ParquetOptions {
                EncryptionKey = Convert.ToBase64String(Enumerable.Range(1, 16).Select(i => (byte)i).ToArray()),
                AADPrefix = "suite-a",
                SupplyAadPrefix = false,     // store prefix in file
                UseCtrVariant = false        // GCM profile
            };

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                var sField = (DataField)schema.Fields[0];
                await rg.WriteColumnAsync(new DataColumn(sField, new[] { "x", "y", "z" }));
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                EncryptionKey = opts.EncryptionKey
            });

            Assert.True(reader.IsEncryptedFile);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var rf = (DataField)reader.Schema.Fields[0];
            DataColumn col = await rgr.ReadColumnAsync(rf);
            Assert.Equal(new[] { "x", "y", "z" }, col.Data);
        }

        [Fact]
        public async Task B_PageEncryption_CtrVariant_RoundTrip() {
            ParquetSchema schema = MakeSchema();
            var opts = new ParquetOptions {
                EncryptionKey = "sixteen-byte-key",  // 16 bytes UTF-8
                AADPrefix = "suite-b",
                SupplyAadPrefix = false,
                UseCtrVariant = true                 // CTR bodies, GCM headers
            };

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                var sField = (DataField)schema.Fields[0];
                await rg.WriteColumnAsync(new DataColumn(sField, Enumerable.Range(0, 100).Select(i => i.ToString()).ToArray()));
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                EncryptionKey = opts.EncryptionKey
            });

            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var rf = (DataField)reader.Schema.Fields[0];
            DataColumn col = await rgr.ReadColumnAsync(rf);
            Assert.Equal(100, col.Data.Length);
            Assert.Equal("0", (string)col.Data.GetValue(0)!);
            Assert.Equal("99", (string)col.Data.GetValue(99)!);
        }

        [Fact]
        public async Task C_PageEncryption_MissingPrefix_Fails_When_SupplyAadPrefix_True() {
            ParquetSchema schema = MakeSchema();
            var opts = new ParquetOptions {
                EncryptionKey = Convert.ToBase64String(Enumerable.Range(1, 32).Select(i => (byte)i).ToArray()),
                AADPrefix = "require-supply",
                SupplyAadPrefix = true,          // do not store prefix in file
                UseCtrVariant = false
            };

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                var sField = (DataField)schema.Fields[0];
                await rg.WriteColumnAsync(new DataColumn(sField, new[] { "a", "b" }));
            }

            ms.Position = 0;
            await Assert.ThrowsAsync<InvalidDataException>(async () => {
                using ParquetReader _ = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                    EncryptionKey = opts.EncryptionKey,
                    AADPrefix = null     // not supplied -> should fail
                });
            });
        }
    }
}
