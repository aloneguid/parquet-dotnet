using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types {
    public class StructureTest : TestBase {

        /// <summary>
        /// This method is used in documentation, keep formatting clear
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task Simple_structure_write_read() {
            var schema = new ParquetSchema(
               new DataField<string>("name"),
               new StructField("address",
                  new DataField<string>("line1"),
                  new DataField<string>("postcode")
               ));

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                ParquetRowGroupWriter rgw = writer.CreateRowGroup();

                await rgw.WriteColumnAsync(
                    new DataColumn((DataField)schema[0], new[] { "Joe" }));

                await rgw.WriteColumnAsync(
                    new DataColumn((DataField)schema[1].NaturalChildren[0], new[] { "Amazonland" }));

                await rgw.WriteColumnAsync(
                    new DataColumn((DataField)schema[1].NaturalChildren[1], new[] { "AAABBB" }));
            }

            ms.Position = 0;

            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);

                DataField[] dataFields = reader.Schema.GetDataFields();

                DataColumn name = await rg.ReadColumnAsync(dataFields[0]);
                DataColumn line1 = await rg.ReadColumnAsync(dataFields[1]);
                DataColumn postcode = await rg.ReadColumnAsync(dataFields[2]);

                Assert.Equal(new[] { "Joe" }, name.Data);
                Assert.Equal(new[] { "Amazonland" }, line1.Data);
                Assert.Equal(new[] { "AAABBB" }, postcode.Data);
            }
        }
    }
}