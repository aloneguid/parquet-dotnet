using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Parquet.Test {
    /// <summary>
    /// Tests a set of predefined test files that they read back correct.
    /// Find more test data (some taken from there): https://github.com/apache/parquet-testing/tree/master/data
    /// </summary>
    public class ParquetReaderOnTestFilesTest : TestBase {

        [Theory]
        [InlineData("fixedlenbytearray.parquet")]
        [InlineData("fixedlenbytearray.v2.parquet")]
        public async Task FixedLenByteArray_dictionary(string parquetFile) {
            using(Stream s = OpenTestFile(parquetFile)) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();
                }
            }
        }

        [Theory]
        [InlineData("dates.parquet")]
        [InlineData("dates.v2.parquet")]
        public async Task Datetypes_all(string parquetFile) {
            DateTime offset, offset2;
            using(Stream s = OpenTestFile(parquetFile)) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();

                    offset = (DateTime)(columns[1].Data.GetValue(0)!);
                    offset2 = (DateTime)(columns[1].Data.GetValue(1)!);
                }
            }

            Assert.Equal(new DateTime(2017, 1, 1), offset.Date);
            Assert.Equal(new DateTime(2017, 2, 1), offset2.Date);
        }

        [Theory]
        [InlineData("datetime_other_system.parquet")]
        [InlineData("datetime_other_system.v2.parquet")]
        public async Task DateTime_FromOtherSystem(string parquetFile) {
            DateTime? offset;
            using(Stream s = OpenTestFile(parquetFile)) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();

                    DataColumn? as_at_date_col = columns.FirstOrDefault(x => x.Field.Name == "as_at_date_");
                    Assert.NotNull(as_at_date_col);

                    offset = (DateTime?)(as_at_date_col.Data.GetValue(0));
                    Assert.Equal(new DateTime(2018, 12, 14, 0, 0, 0), offset!.Value.Date);
                }
            }
        }

        private async Task OptionalValues_WithoutStatistics(string parquetFile) {
            using(Stream s = OpenTestFile(parquetFile)) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();
                    DataColumn? id_col = columns.FirstOrDefault(x => x.Field.Name == "id");
                    DataColumn? value_col = columns.FirstOrDefault(x => x.Field.Name == "value");
                    Assert.NotNull(id_col);
                    Assert.NotNull(value_col);

                    int index = Enumerable.Range(0, id_col.Data.Length)
                        .First(i => (long)id_col.Data.GetValue(i)! == 20908539289);

                    Assert.Equal(0, value_col.Data.GetValue(index));
                }
            }
        }

        [Theory]
        [InlineData("issue-164.parquet")]
        [InlineData("issue-164.v2.parquet")]
        public async Task Issue164(string parquetFile) {
            using(Stream s = OpenTestFile(parquetFile)) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();
                    DataColumn id_col = columns[0];
                    DataColumn cls_value_8 = columns[9];
                    int index = Enumerable.Range(0, id_col.Data.Length)
                        .First(i => (int)id_col.Data.GetValue(i)! == 256779);
                    Assert.Equal("MOSTRU\u00C1RIO-000", cls_value_8.Data.GetValue(index));

                }
            }
        }

        [Fact]
        public async Task ByteArrayDecimal() {
            using Stream s = OpenTestFile("byte_array_decimal.parquet");
            using ParquetReader r = await ParquetReader.CreateAsync(s);

            ParquetSchema schema = r.Schema;
            Assert.Equal("value", schema[0].Name);

            DataColumn[] cols = await r.ReadEntireRowGroupAsync();
            Assert.Single(cols);
            DataColumn dc = cols[0];

            Assert.Equal(
                Enumerable.Range(1, 24).Select(i => (decimal?)i),
                (decimal?[])dc.Data);
        }

        [Fact]
        public async Task Read_delta_binary_packed() {
            using Stream s = OpenTestFile("delta_binary_packed.parquet");
            using ParquetReader r = await ParquetReader.CreateAsync(s);

            ParquetSchema schema = r.Schema;

            using(ParquetRowGroupReader rgr = r.OpenRowGroupReader(0)) {
                DataField[] dfs = schema.GetDataFields();

                DataColumn bw1 = await rgr.ReadColumnAsync(dfs[1]);
                Assert.Equal(200, bw1.NumValues);
            }
        }

        [Fact]
        public async Task Read_col_names_with_trailing_dots() {
            using Stream s = OpenTestFile("trailing_dot_col_name.parquet");
            Table tbl = await ParquetReader.ReadTableFromStreamAsync(s);
            Assert.NotNull(tbl);
        }

        [Fact]
        public async Task Read_legacy_list() {
            using Stream s = OpenTestFile("special/legacy-list.parquet");
            using ParquetReader r = await ParquetReader.CreateAsync(s);
            DataColumn[] cols = await r.ReadEntireRowGroupAsync();

            Assert.Equal(3, cols.Length);
            Assert.Equal(new string[] { "1_0", "1_0" }, cols[0].Data);
            Assert.Equal(new double[] { 2004, 2004 }, cols[1].Data);
            Assert.Equal(Enumerable.Range(0, 168).Concat(Enumerable.Range(0, 168)).ToArray(), cols[2].Data);
        }

        [Fact]
        public async Task Read_empty_and_null_lists() {
            using Stream s = OpenTestFile("list_empty_and_null.parquet");
            List<DataColumn> cols = await ReadColumns(s);
            Assert.Equal(2, cols.Count);
        }

        [Fact]
        public async Task Wide() {
            using Stream s = OpenTestFile("special/wide.parquet");
            List<DataColumn> cols = await ReadColumns(s);
        }

        //[Fact]
        //public async Task LocalTest() {
        //    using Stream s = System.IO.File.OpenRead(@"C:\Users\alone\Downloads\MAP_FIELD\MAP_FIELD.parquet");
        //    List<DataColumn> cols = await ReadColumns(s);
        //    Assert.Equal(3, cols.Count);
        //}
    }
}