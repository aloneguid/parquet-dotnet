using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class MixedAlgorithmTests {

#if NET6_0_OR_GREATER || NETSTANDARD2_1
        [Fact]
        public async Task CtrVariant_Roundtrip_Works() {
            var schema = new ParquetSchema(new DataField<int>("v"));
            var data = new DataColumn((DataField)schema.Fields[0], Enumerable.Range(0, 10_000).ToArray());

            var writeOpts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = "00112233445566778899AABBCCDDEEFF",
                UseCtrVariant = true
            };

            byte[] bytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, writeOpts)) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(data);
                }
                bytes = ms.ToArray();
            }

            var readOpts = new ParquetOptions { FooterSigningKey = writeOpts.FooterSigningKey };
            int[] vals;
            using(var ms = new MemoryStream(bytes)) {
                using ParquetReader r = await ParquetReader.CreateAsync(ms, readOpts);
                using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);
                DataColumn col = await rg.ReadColumnAsync((DataField)r.Schema.Fields[0]);
                vals = col.AsSpan<int>().ToArray();
            }

            Assert.Equal(Enumerable.Range(0, 10_000), vals);
        }
#endif

    }
}
