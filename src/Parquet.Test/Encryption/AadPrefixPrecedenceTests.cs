using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class AadPrefixPrecedenceTests : TestBase {

        private static ParquetSchema MakeSchema()
            => new ParquetSchema(new DataField<string>("s"));

        private static DataColumn MakeColumn(ParquetSchema schema, params string[] values) {
            var sField = (DataField)schema.Fields[0];
            return new DataColumn(sField, values);
        }

        private static string B64Key(int bytes = 16)
            => Convert.ToBase64String(Enumerable.Range(1, bytes).Select(i => (byte)i).ToArray());

        [Theory]
        [InlineData(false)] // AES-GCM-V1
        [InlineData(true)]  // AES-GCM-CTR-V1
        public async Task StoredPrefix_Ignores_Wrong_Supplied_At_Read(bool useCtr) {
            // Write: store the prefix in the file (SupplyAadPrefix=false)
            string key = B64Key(16);
            string storedPrefix = "stored-prefix-precedence";
            var writeOpts = new ParquetOptions {
                FooterEncryptionKey = key,
                AADPrefix = storedPrefix,
                SupplyAadPrefix = false,  // store in file
                UseCtrVariant = useCtr
            };

            ParquetSchema schema = MakeSchema();
            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: writeOpts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(MakeColumn(schema, "a", "b", "c"));
            }

            // Read: deliberately pass a WRONG prefix; it must be ignored because file stores its own.
            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                FooterEncryptionKey = key,
                AADPrefix = "WRONG-WRONG-WRONG", // should not matter
            });

            Assert.True(reader.IsEncryptedFile);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var df = (DataField)reader.Schema.Fields[0];
            DataColumn col = await rgr.ReadColumnAsync(df);

            Assert.Equal(new[] { "a", "b", "c" }, col.Data);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task StoredPrefix_Also_Works_When_Supplied_Is_Null(bool useCtr) {
            // Control: showing that a null supplied prefix also works (already covered in other tests,
            // but included here for completeness with the same writer settings).
            string key = B64Key(32);
            var writeOpts = new ParquetOptions {
                FooterEncryptionKey = key,
                AADPrefix = "stored-AAD-here",
                SupplyAadPrefix = false,
                UseCtrVariant = useCtr
            };

            ParquetSchema schema = MakeSchema();
            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: writeOpts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(MakeColumn(schema, "x", "y", "z"));
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                FooterEncryptionKey = key,
                AADPrefix = null
            });

            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var df = (DataField)reader.Schema.Fields[0];
            DataColumn col = await rgr.ReadColumnAsync(df);
            Assert.Equal(new[] { "x", "y", "z" }, col.Data);
        }
    }
}
