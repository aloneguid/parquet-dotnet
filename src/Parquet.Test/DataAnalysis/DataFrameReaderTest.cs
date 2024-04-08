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
        [InlineData(typeof(short), (short)1, (short)2)]
        [InlineData(typeof(short?), null, (short)2)]
        [InlineData(typeof(int), 1, 2)]
        [InlineData(typeof(int?), null, 2)]
        [InlineData(typeof(bool), true, false)]
        [InlineData(typeof(bool?), true, null)]
        [InlineData(typeof(long), 1L, 2L)]
        [InlineData(typeof(long?), 1L, 2L)]
        [InlineData(typeof(ulong), 1UL, 2UL)]
        [InlineData(typeof(ulong?), 1UL, 2UL)]
        [InlineData(typeof(string), "1", "2")]
        [InlineData(typeof(string), null, "2")]
        public async Task Roundtrip_all_types(Type t, object? el1, object? el2) {

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

            if(t == typeof(long)) {
                // Int64 is a special case in DataFrame
                // see https://github.com/aloneguid/parquet-dotnet/issues/343 for more info
                df1.Columns.GetInt64Column(t.Name);
            } else if (t == typeof(ulong)) {
                df1.Columns.GetUInt64Column(t.Name);
            }

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

        [Fact]
        public async Task Read_multiple_row_groups() {
            // generate file with multiple row groups

            var ms = new MemoryStream();
            var id = new DataField<int>("id");

            //write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 5, 6 }));
                }
            }

            // read as DataFrame
            ms.Position = 0;
            DataFrame df = await ms.ReadParquetAsDataFrameAsync();

            // check that all the values are present
            Assert.Equal(6, df.Rows.Count);
            Assert.Equal(1, df.Rows[0][0]);
            Assert.Equal(2, df.Rows[1][0]);
        }
    }
}
