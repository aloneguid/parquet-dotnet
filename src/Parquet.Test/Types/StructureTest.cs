using System.Collections.Generic;
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
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

                await rgw.WriteAsync(schema.DataFields[0], new[] { "Joe" });

                await rgw.WriteAsync(schema.DataFields[1], new[] { "Amazonland" });

                await rgw.WriteAsync(schema.DataFields[2], new[] { "AAABBB" });
            }

            ms.Position = 0;

            await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);

                DataField[] dataFields = reader.Schema.GetDataFields();

                List<string?> names = await ReadStringColumn(reader, dataFields[0]);
                List<string?> line1s = await ReadStringColumn(reader, dataFields[1]);
                List<string?> postcodes = await ReadStringColumn(reader, dataFields[2]);

                Assert.Equal(new[] { "Joe" }, names);
                Assert.Equal(new[] { "Amazonland" }, line1s);
                Assert.Equal(new[] { "AAABBB" }, postcodes);
            }
        }
    }
}