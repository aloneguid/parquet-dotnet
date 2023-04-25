using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Rows {
    public class RowsModelTest : TestBase {
        #region [ Flat Tables ]

        [Fact]
        public void Flat_add_valid_row_succeeds() {
            var table = new Table(new ParquetSchema(new DataField<int>("id")));
            table.Add(new Row(1));
        }

        [Fact]
        public void Flat_add_invalid_type_fails() {
            var table = new Table(new ParquetSchema(new DataField<int>("id")));

            Assert.Throws<ArgumentException>(() => table.Add(new Row("1")));
        }

        [Fact]
        public async Task Flat_write_read() {
            var table = new Table(new ParquetSchema(new DataField<int>("id"), new DataField<string>("city")));
            var ms = new MemoryStream();

            //generate fake data
            for(int i = 0; i < 1000; i++) {
                table.Add(new Row(i, "record#" + i));
            }

            //write to stream
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(table.Schema, ms)) {
                await writer.WriteAsync(table);
            }

            //read back into table
            ms.Position = 0;
            Table table2;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                table2 = await reader.ReadAsTableAsync();
            }

            //validate data
            Assert.True(table.Equals(table2, true));
        }

        [Fact]
        public async Task Flat_concurrent_write_read() {
            var schema = new ParquetSchema(
               new DataField<int>("id"),
               new DataField<string>("city"));

            var inputTables = new List<Table>();
            for(int i = 0; i < 10; i++) {
                var table = new Table(schema);
                for(int j = 0; j < 10; j++) {
                    table.Add(new Row(i, $"record#{i},{j}"));
                }

                inputTables.Add(table);
            }

            Task<Table>[] tasks = inputTables
                  .Select(t => Task.Run(() => WriteReadAsync(t)))
                  .ToArray();

            Table[] outputTables = await Task.WhenAll(tasks);

            for(int i = 0; i < inputTables.Count; i++) {
                Assert.True(inputTables[i].Equals(outputTables[i], true));
            }
        }

        #endregion

        #region [ Array Tables ]

        [Fact]
        public void Array_validate_succeeds() {
            var table = new Table(new ParquetSchema(new DataField<IEnumerable<int>>("ids")));

            table.Add(new Row(new[] { 1, 2, 3 }));
            table.Add(new Row(new[] { 4, 5, 6 }));
        }

        [Fact]
        public void Array_validate_fails() {
            var table = new Table(new ParquetSchema(new DataField<IEnumerable<int>>("ids")));

            Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
        }

        [Fact]
        public async Task Array_write_read() {
            var table = new Table(
               new ParquetSchema(
                  new DataField<int>("id"),
                  new DataField<string[]>("categories")     //array field
                  )
               );
            var ms = new MemoryStream();

            table.Add(1, new[] { "1", "2", "3" });
            table.Add(3, new[] { "3", "3", "3" });

            //write to stream
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(table.Schema, ms)) {
                await writer.WriteAsync(table);
            }

            //System.IO.File.WriteAllBytes("c:\\tmp\\array.parquet", ms.ToArray());

            //read back into table
            ms.Position = 0;
            Table table2;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                table2 = await reader.ReadAsTableAsync();
            }

            //validate data
            Assert.Equal(table.ToString(), table2.ToString(), ignoreLineEndingDifferences: true);
        }

        #endregion

        #region [ Maps ]

        [Fact]
        public void Map_validate_succeeds() {
            var table = new Table(new ParquetSchema(
               new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
               ));

            table.Add(Row.SingleCell(
               new List<Row>
               {
               new Row("one", "v1"),
               new Row("two", "v2")
               }));
        }

        [Fact]
        public void Map_validate_fails() {
            var table = new Table(new ParquetSchema(
               new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
               ));

            Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
        }

        [Fact]
        public async Task Map_read_from_Apache_Spark() {
            Table t;
            using(Stream stream = OpenTestFile("map_simple.parquet")) {
                using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
                    t = await reader.ReadAsTableAsync();
                }
            }

            Assert.Equal("{'id': 1, 'numbers': [{'key': 1, 'value': 'one'}, {'key': 2, 'value': 'two'}, {'key': 3, 'value': 'three'}]}", t[0].ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task Map_write_read() {
            var table = new Table(
               new ParquetSchema(
                  new DataField<string>("city"),
                  new MapField("population",
                     new DataField<int>("areaId"),
                     new DataField<long>("count"))));
            var ms = new MemoryStream();

            table.Add("London",
               new List<Row>
               {
               new Row(234, 100L),
               new Row(235, 110L)
               });

            Table table2 = await WriteReadAsync(table);

            Assert.Equal(table.ToString(), table2.ToString(), ignoreLineEndingDifferences: true);
        }

        #endregion

        #region [ Struct ]

        [Fact]
        public async Task Struct_read_plain_structs_from_Apache_Spark() {
            Table t = await ReadTestFileAsTableAsync("struct_plain.parquet");

            Assert.Equal(@"{'isbn': '12345-6', 'author': {'firstName': 'Ivan', 'lastName': 'Gavryliuk'}}
{'isbn': '12345-7', 'author': {'firstName': 'Richard', 'lastName': 'Conway'}}", t.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task Struct_write_read() {
            var table = new Table(
               new ParquetSchema(
                  new DataField<string>("isbn"),
                  new StructField("author",
                     new DataField<string>("firstName"),
                     new DataField<string>("lastName"))));
            var ms = new MemoryStream();

            table.Add("12345-6", new Row("Hazel", "Nut"));
            table.Add("12345-7", new Row("Marsha", "Mellow"));

            Table table2 = await WriteReadAsync(table);

            Assert.Equal(table.ToString(), table2.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task Struct_write_read_with_null_entry() {
            var table = new Table(
               new ParquetSchema(
                  new DataField<string>("isbn"),
                  new StructField("author",
                     new DataField<string>("firstName"),
                     new DataField<string>("lastName"))));
            var ms = new MemoryStream();

            table.Add("12345-6", new Row("Hazel", "Nut"));
            table.Add("12345-7", new Row("Marsha", "Mellow"));
            table.Add("12345-7", null);

            Table table2 = await WriteReadAsync(table);

            // temporary hack as null entry structures are not well supported
            var tmp = new Table(table.Schema);
            tmp.Add(table[0]);
            tmp.Add(table[1]);
            tmp.Add("12345-7", new Row(new object?[] { null, null }));

            Assert.Equal(tmp.ToString(), table2.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task Struct_with_repeated_field_writes_reads() {
            var t = new Table(new ParquetSchema(
               new DataField<string>("name"),
               new StructField("address",
                  new DataField<string>("name"),
                  new DataField<IEnumerable<string>>("lines"))));

            t.Add("Hazel", new Row("Primary", new[] { "line1", "line2" }));

            Table t2 = await WriteReadAsync(t);

            Assert.Equal("{'name': 'Hazel', 'address': {'name': 'Primary', 'lines': ['line1', 'line2']}}", t2.ToString(), ignoreLineEndingDifferences: true);
        }

        #endregion

        #region [ List ]

        [Fact]
        public void List_table_equality() {
            var schema = new ParquetSchema(new ListField("ints", new DataField<int>("int")));


            var tbl1 = new Table(schema);
            tbl1.Add(Row.SingleCell(new[] { 1, 1, 1 }));

            var tbl2 = new Table(schema);
            tbl2.Add(Row.SingleCell(new[] { 1, 1, 1 }));

            Assert.True(tbl1.Equals(tbl2, true));
        }

        [Fact]
        public async Task List_read_simple_element_from_Apache_Spark() {
            Table t;
            using(Stream stream = OpenTestFile("list_simple.parquet")) {
                using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
                    t = await reader.ReadAsTableAsync();
                }
            }

            Assert.Equal("{'cities': ['London', 'Derby', 'Paris', 'New York'], 'id': 1}", t[0].ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_simple_element_write_read() {
            var table = new Table(
               new ParquetSchema(
                  new DataField<int>("id"),
                  new ListField("cities",
                     new DataField<string>("name"))));

            var ms = new MemoryStream();

            table.Add(1, new[] { "London", "Derby" });
            table.Add(2, new[] { "Paris", "New York" });

            //write as table
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(table.Schema, ms)) {
                await writer.WriteAsync(table);
            }

            //read back into table
            ms.Position = 0;
            Table table2;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                table2 = await reader.ReadAsTableAsync();
            }

            //validate data
            Assert.Equal(table.ToString(), table2.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_read_structures_from_Apache_Spark() {
            Table t;
            using(Stream stream = OpenTestFile("list_structs.parquet")) {
                using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
                    t = await reader.ReadAsTableAsync();
                }
            }

            Assert.Single(t);
            Assert.Equal("{'cities': [{'country': 'UK', 'name': 'London'}, {'country': 'US', 'name': 'New York'}], 'id': 1}", t[0].ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_read_write_structures() {
            Table t = new Table(
               new DataField<int>("id"),
               new ListField("structs",
                  new StructField("mystruct",
                     new DataField<int>("id"),
                     new DataField<string>("name"))));

            t.Add(1, new[] { new Row(1, "Joe"), new Row(2, "Bloggs") });
            t.Add(2, new[] { new Row(3, "Star"), new Row(4, "Wars") });

            Table t2 = await WriteReadAsync(t);

            Assert.Equal(t.ToString(), t2.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_of_elements_is_empty_read_from_Apache_Spark() {
            Table t = await ReadTestFileAsTableAsync("list_empty.parquet");

            Assert.Equal("{'id': 2, 'repeats1': []}", t[0].ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_of_elements_empty_alternates_read_from_Apache_Spark() {
            /*
             list data:
             - 1: [1, 2, 3]
             - 2: []
             - 3: [1, 2, 3]
             - 4: []
             */

            Table t = await ReadTestFileAsTableAsync("list_empty_alt.parquet");

            Assert.Equal(@"{'id': 1, 'repeats2': ['1', '2', '3']}
{'id': 2, 'repeats2': []}
{'id': 3, 'repeats2': ['1', '2', '3']}
{'id': 4, 'repeats2': []}", t.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_of_elements_is_empty_writes_reads() {
            var t = new Table(
               new DataField<int>("id"),
               new ListField("strings",
                  new DataField<string>("item")
               ));
            t.Add(1, new string[0]);
            Assert.Equal("{'id': 1, 'strings': []}", (await WriteReadAsync(t)).ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_of_elements_with_some_items_empty_writes_reads() {
            var t = new Table(
               new DataField<int>("id"),
               new ListField("strings",
                  new DataField<string>("item")
               ));
            t.Add(1, new string[] { "1", "2", "3" });
            t.Add(2, new string[] { });
            t.Add(3, new string[] { "1", "2", "3" });
            t.Add(4, new string[] { });

            Table t1 = await WriteReadAsync(t);
            Assert.Equal(4, t1.Count);
            Assert.Equal(@"{'id': 1, 'strings': ['1', '2', '3']}
{'id': 2, 'strings': []}
{'id': 3, 'strings': ['1', '2', '3']}
{'id': 4, 'strings': []}", t.ToString(), ignoreLineEndingDifferences: true);

        }

        [Fact]
        public async Task List_of_lists_read_write_structures() {
            var t = new Table(
                new DataField<int>("id"),
                new ListField(
                    "items",
                    new StructField(
                        "item",
                        new DataField<int>("id"),
                        new ListField(
                            "values",
                            new DataField<string>("value")))));

            t.Add(0, new[] {
                new Row(0, new[] { "0,0,0", "0,0,1", "0,0,2" }),
                new Row(1, new[] { "0,1,0", "0,1,1", "0,1,2" }),
                new Row(2, new[] { "0,2,0", "0,2,1", "0,2,2" }),
             });

            Table t1 = await WriteReadAsync(t);
            Assert.Equal(3, ((object[])t1[0]![1]!).Length);
            Assert.Equal(t.ToString(), t1.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_of_structures_is_empty_read_write_structures() {
            Table t = new Table(
               new DataField<int>("id"),
               new ListField("structs",
                  new StructField("mystruct",
                     new DataField<int>("id"),
                     new DataField<string>("name"))));

            t.Add(1, new Row[0]);
            t.Add(2, new Row[0]);

            Table t2 = await WriteReadAsync(t);

            Assert.Equal(t.ToString(), t2.ToString(), ignoreLineEndingDifferences: true);
        }

        [Fact]
        public async Task List_of_structures_is_empty_as_first_field_read_write_structures() {
            Table t = new Table(
               new ListField("structs",
                  new StructField("mystruct",
                     new DataField<int>("id"),
                     new DataField<string>("name"))),
               new DataField<int>("id"));

            t.Add(new Row[0], 1);
            t.Add(new Row[0], 2);

            Table t2 = await WriteReadAsync(t);

            Assert.Equal(t.ToString(), t2.ToString(), ignoreLineEndingDifferences: true);
        }

        #endregion

        #region [ Mixed ]

        #endregion

        #region [ Special Cases ]

        [Fact]
        public async Task Special_read_all_nulls_no_booleans() {
            await ReadTestFileAsTableAsync("special/all_nulls_no_booleans.parquet");
        }

        [Theory]
        [InlineData("special/all_nulls.parquet")]
        [InlineData("special/all_nulls.v2.parquet")]
        public async Task Special_all_nulls_file(string parquetFile) {
            Table t = await ReadTestFileAsTableAsync(parquetFile);

            Assert.Equal(1, t.Schema.Fields.Count);
            Assert.Equal("lognumber", t.Schema[0].Name);
            Assert.Single(t);
            Assert.Null(t[0][0]);
        }

        [Theory]
        [InlineData("special/decimalnulls.parquet")]
        [InlineData("special/decimalnulls.v2.parquet")]
        public async Task Special_read_all_nulls_decimal_column(string parquetFile) {
            await ReadTestFileAsTableAsync(parquetFile);
        }

        [Theory]
        [InlineData("special/decimallegacy.parquet")]
        [InlineData("special/decimallegacy.v2.parquet")]
        public async Task Special_read_all_legacy_decimals(string parquetFile) {
            Table ds = await ReadTestFileAsTableAsync(parquetFile);

            Row row = ds[0];
            Assert.Equal(1, (int)row[0]!);
            Assert.Equal(1.2m, (decimal)row[1]!, 2);
            Assert.Null(row[2]);
            Assert.Equal(-1m, (decimal)row[3]!, 2);
        }

        [Fact]
        public async Task Special_read_file_with_multiple_RowGroups() {
            var ms = new MemoryStream();

            //create multirowgroup file

            //first row group
            var t = new Table(new DataField<int>("id"));
            t.Add(1);
            t.Add(2);
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(t.Schema, ms)) {
                await writer.WriteAsync(t);
            }

            //second row group
            t.Clear();
            t.Add(3);
            t.Add(4);
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(t.Schema, ms, null, true)) {
                await writer.WriteAsync(t);
            }

            //read back as table
            t = await ParquetReader.ReadTableFromStreamAsync(ms);
            Assert.Equal(4, t.Count);
        }

        #endregion

        #region [ And the Big Ultimate Fat Test!!! ]

        /// <summary>
        /// This essentially proves that we can READ complicated data structures, but not write (see other tests)
        /// </summary>
        [Fact]
        public async Task BigFatOne_variations_from_Apache_Spark() {
            Table t;
            using(Stream stream = OpenTestFile("all_var1.parquet")) {
                using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
                    t = await reader.ReadAsTableAsync();
                }
            }

            Assert.Equal(2, t.Count);
            Assert.Equal("{'addresses': [{'line1': 'Dante Road', 'name': 'Head Office', 'openingHours': [9, 10, 11, 12, 13, 14, 15, 16, 17, 18], 'postcode': 'SE11'}, {'line1': 'Somewhere Else', 'name': 'Small Office', 'openingHours': [6, 7, 19, 20, 21, 22, 23], 'postcode': 'TN19'}], 'cities': ['London', 'Derby'], 'comment': 'this file contains all the permunations for nested structures and arrays to test Parquet parser', 'id': 1, 'location': {'latitude': 51.2, 'longitude': 66.3}, 'price': {'lunch': {'max': 2, 'min': 1}}}", t[0].ToString());
            Assert.Equal("{'addresses': [{'line1': 'Dante Road', 'name': 'Head Office', 'openingHours': [9, 10, 11, 12, 13, 14, 15, 16, 17, 18], 'postcode': 'SE11'}, {'line1': 'Somewhere Else', 'name': 'Small Office', 'openingHours': [6, 7, 19, 20, 21, 22, 23], 'postcode': 'TN19'}], 'cities': ['London', 'Derby'], 'comment': 'this file contains all the permunations for nested structures and arrays to test Parquet parser', 'id': 1, 'location': {'latitude': 51.2, 'longitude': 66.3}, 'price': {'lunch': {'max': 2, 'min': 1}}}", t[1].ToString());
        }

        #endregion

        #region [ JSON Conversions ]

        [Theory]
        [InlineData("struct_plain.parquet")]
        [InlineData("struct_plain.v2.parquet")]
        public async Task JSON_struct_plain_reads_by_newtonsoft(string parquetFile) {
            Table t = await ReadTestFileAsTableAsync(parquetFile);

            Assert.Equal("{'isbn': '12345-6', 'author': {'firstName': 'Ivan', 'lastName': 'Gavryliuk'}}", t[0].ToString("jsq"));
            //Assert.Equal("{'isbn': '12345-7', 'author': {'firstName': 'Marsha', 'lastName': 'Mellow'}}", t[1].ToString("jsq"));

            string[] jsons = t.ToString("j").Split(new[] { Environment.NewLine }, StringSplitOptions.None);

            jsons.Select(j => JsonConvert.DeserializeObject(j)).ToList();
        }

        [Fact]
        public void JSON_convert_with_null_arrays() {
            var t = new Table(new ParquetSchema(
               new DataField<string>("name"),
               new ListField("observations",
                  new StructField("observation",
                     new DataField<DateTime>("date"),
                     new DataField<bool>("bool"),
                     new DataField<IEnumerable<int?>>("values")))));

            var defaultDate = new DateTime(2018, 1, 1, 1, 1, 1, DateTimeKind.Utc);

            t.Add("London", new[] {
                new Row(defaultDate, true, new int?[] { 1, 2, 3, 4, 5 })
            });

            t.Add("Oslo", new[] {
                new Row(defaultDate, false, new int?[] { null, null, null, null, null, null, null })
            });

            string[] jsons = t.ToString("jsq").Split(new[] { Environment.NewLine }, StringSplitOptions.None);
            Assert.Equal($"{{'name': 'London', 'observations': [{{'date': '{defaultDate}', 'bool': true, 'values': [1, 2, 3, 4, 5]}}]}}", jsons[0]);
            Assert.Equal($"{{'name': 'Oslo', 'observations': [{{'date': '{defaultDate}', 'bool': false, 'values': [null, null, null, null, null, null, null]}}]}}", jsons[1]);
        }

        #endregion

        #region [ Callback ]

        [Fact]
        public async Task Progress_callback_is_called() {
            Table t;
            int timesInvoked = 0;
            var messages = new List<string>();
            using(Stream stream = OpenTestFile("customer.impala.parquet")) {
                using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
                    t = await reader.ReadAsTableAsync((int perc, string msg) => {
                        Console.WriteLine($"{perc}%: {msg}");
                        timesInvoked++;
                        messages.Add(msg);
                        return Task.CompletedTask;
                    });
                }
            }

            Assert.NotNull(t);
            Assert.True(timesInvoked > 0);
        }


        #endregion
    }
}