// src/Parquet.Test/Encryption/Writer_192bitKey_Tests.cs
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class Writer_192bitKey_Tests : TestBase {

        [Fact(DisplayName = "EF: write & read with AES-192 key in this pipeline")]
        public async Task EF_Aes192_RoundTrip() {
            string k192 = Convert.ToBase64String(Enumerable.Range(1, 24).Select(i => (byte)i).ToArray());
            var schema = new ParquetSchema(new DataField<string>("s"));

            byte[] file;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, new ParquetOptions {
                    FooterEncryptionKey = k192,
                    AADPrefix = "k192-ef",
                    SupplyAadPrefix = false
                })) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { "x", "y" }));
                }
                file = ms.ToArray();
            }

            using var ms2 = new MemoryStream(file, writable: false);
            using ParquetReader r = await ParquetReader.CreateAsync(ms2, new ParquetOptions {
                FooterEncryptionKey = k192
            });
            using ParquetRowGroupReader gr = r.OpenRowGroupReader(0);
            var df = (DataField)r.Schema.Fields[0];
            DataColumn col = await gr.ReadColumnAsync(df);
            Assert.Equal(new[] { "x", "y" }, (string[])col.Data);
        }

        [Fact(DisplayName = "PF: write & read with AES-192 (signed footer)")]
        public async Task PF_Aes192_RoundTrip() {
            string k192 = Convert.ToBase64String(Enumerable.Range(1, 24).Select(i => (byte)i).ToArray());
            var schema = new ParquetSchema(new DataField<int>("n"));

            byte[] file;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, new ParquetOptions {
                    UsePlaintextFooter = true,
                    FooterSigningKey = k192,
                    AADPrefix = "k192-pf",
                    SupplyAadPrefix = false
                })) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3 }));
                }
                file = ms.ToArray();
            }

            using var ms2 = new MemoryStream(file, writable: false);
            using ParquetReader r = await ParquetReader.CreateAsync(ms2, new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = k192
            });
            using ParquetRowGroupReader gr = r.OpenRowGroupReader(0);
            var df = (DataField)r.Schema.Fields[0];
            DataColumn col = await gr.ReadColumnAsync(df);
            Assert.Equal(new[] { 1, 2, 3 }, (int[])col.Data);
        }
    }
}
