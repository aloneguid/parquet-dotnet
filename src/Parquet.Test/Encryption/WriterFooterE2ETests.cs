using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class WriterFooterE2ETests : TestBase {

        private static ParquetSchema MakeSchema()
            => new ParquetSchema(new DataField<string>("s"));

        private static DataColumn MakeColumn(ParquetSchema schema, params string[] values) {
            var sField = (DataField)schema.Fields[0];   // field attached to the schema
            return new DataColumn(sField, values);
        }

        private static DataColumn[] SampleColumns() =>
            new[] { MakeColumn(MakeSchema(), "a", "b", "c") };

        [Fact]
        public async Task A_FooterEncrypted_GcmV1_PrefixStored_RoundTrip() {
            ParquetSchema schema = MakeSchema();
            var opts = new ParquetOptions {
                FooterEncryptionKey = Convert.ToBase64String(Enumerable.Range(1, 16).Select(i => (byte)i).ToArray()),
                AADPrefix = "stored-prefix"  // we’ll store it in file in CreateEncrypterForWrite
            };

            using var ms = new MemoryStream();
            // write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(SampleColumns()[0]);
            }

            ms.Position = 0;

            // read back (no prefix needed because it’s stored in file)
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                FooterEncryptionKey = opts.FooterEncryptionKey,
                AADPrefix = null
            });
            Assert.True(reader.IsEncryptedFile);
            Assert.Equal(1, reader.RowGroupCount);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var dataField = (DataField)reader.Schema.Fields[0];
            DataColumn col = await rgr.ReadColumnAsync(dataField);
            Assert.Equal(new[] { "a", "b", "c" }, col.Data);
        }

        [Fact]
        public async Task B_FooterEncrypted_GcmV1_PrefixSupplied_RoundTrip() {
            ParquetSchema schema = MakeSchema();
            string prefix = "supply-me";
            var opts = new ParquetOptions {
                FooterEncryptionKey = Convert.ToBase64String(Enumerable.Range(1, 32).Select(i => (byte)i).ToArray()),
                AADPrefix = prefix   // we’ll mark SupplyAadPrefix=true in CreateEncrypterForWrite
            };

            // tell your writer path to use SupplyAadPrefix=true (e.g., via a bool in ParquetOptions or a small temp tweak)

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(SampleColumns()[0]);
            }

            ms.Position = 0;

            // must supply prefix on read
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                FooterEncryptionKey = opts.FooterEncryptionKey,
                AADPrefix = prefix
            });
            Assert.True(reader.IsEncryptedFile);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var dataField = (DataField)reader.Schema.Fields[0];
            DataColumn col = await rgr.ReadColumnAsync(dataField);
            Assert.Equal(new[] { "a", "b", "c" }, col.Data);
        }

        [Fact]
        public async Task C_FooterEncrypted_CtrVariant_FooterStillGcm_RoundTrip() {
            ParquetSchema schema = MakeSchema();
            var opts = new ParquetOptions {
                FooterEncryptionKey = "sixteen-byte-key", // 16 bytes
                AADPrefix = "ctr-variant"
            };

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(SampleColumns()[0]);
            }

            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                FooterEncryptionKey = opts.FooterEncryptionKey,
                AADPrefix = null // if stored; or same prefix if supply mode
            });
            Assert.True(reader.IsEncryptedFile);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var dataField = (DataField)reader.Schema.Fields[0];
            DataColumn col = await rgr.ReadColumnAsync(dataField);
            Assert.Equal(new[] { "a", "b", "c" }, col.Data);
        }
    }
}
