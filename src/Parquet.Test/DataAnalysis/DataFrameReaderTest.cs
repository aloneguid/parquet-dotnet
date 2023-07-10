using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Analysis;
using Parquet.Encodings;
using Parquet.Schema;
using Xunit;
using System.Linq;
using Parquet.Data;

namespace Parquet.Test.DataAnalysis {
    public class DataFrameReaderTest : TestBase {

        [Theory]
        [InlineData(typeof(int), 1, 2)]
        [InlineData(typeof(int?), null, 2)]
        [InlineData(typeof(bool), true, false)]
        [InlineData(typeof(bool?), true, null)]
        [InlineData(typeof(string), "1", "2")]
        [InlineData(typeof(string), null, "2")]
        public async Task Roundtrip_all_types(Type t, object el1, object el2) {

            // arrange
            using var ms = new MemoryStream();
            var data = Array.CreateInstance(t, 2);
            data.SetValue(el1, 0);
            data.SetValue(el2, 1);


            // make schema
            var schema = new ParquetSchema(new DataField(t.Name, t));

            // make data
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

                var dc = new DataColumn(schema.DataFields[0], data);

                await rgw.WriteColumnAsync(dc);
            }

            // read as DataFrame
            ms.Position = 0;
            DataFrame df = await ms.ReadParquetAsDataFrameAsync();

            Assert.Equal(data, df.Rows.Select(r => r[0]).ToArray());

            // write DataFrame to file
            using var ms1 = new MemoryStream();
            await df.WriteAsync(ms1);

            // validate both are the same
            ms1.Position = 0;
            DataFrame df1 = await ms1.ReadParquetAsDataFrameAsync();

            Assert.Equal(df.Columns.Count, df1.Columns.Count);
            for(int i = 0; i < df.Columns.Count; i++) {
                Assert.Equal(df.Columns[i], df1.Columns[i]);
            }
        }

        [Fact]
        public async Task Read_postcodes_file() {
            using Stream fs = OpenTestFile("postcodes.plain.parquet");
            DataFrame df = await fs.ReadParquetAsDataFrameAsync();
        }

        [Fact]
        public async Task Read_nested_file() {
            using Stream fs = OpenTestFile("simplenested.parquet");
            DataFrame df = await fs.ReadParquetAsDataFrameAsync();
        }
    }
}
