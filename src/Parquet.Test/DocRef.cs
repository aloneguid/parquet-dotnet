using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {

    class Record {
        public DateTime Timestamp { get; set; }

        public string? EventName { get; set; }

        public double MeterValue { get; set; }
    }

    public class DocTest {

        //[Fact]
        public async Task Write1() {
            var data = Enumerable.Range(0, 1_000_000).Select(i => new Record {
                Timestamp = DateTime.UtcNow.AddSeconds(i),
                EventName = i % 2 == 0 ? "on" : "off",
                MeterValue = i 
            }).ToList();

            System.IO.File.Delete("c:\\tmp\\data.parquet");
            await ParquetConvert.SerializeAsync(data, "c:\\tmp\\data.parquet");

            Record[] data2 = await ParquetConvert.DeserializeAsync<Record>("c:\\tmp\\data.parquet");
            Assert.NotNull(data2);
        }

        //[Fact]
        public async Task Write2() {
            var table = new Table(
                new DataField<DateTime>("Timestamp"),
                new DataField<string>("EventName"),
                new DataField<double>("MeterName"));

            for(int i = 0; i < 10; i++) {
                table.Add(
                    DateTime.UtcNow.AddSeconds(1),
                    i % 2 == 0 ? "on" : "off",
                    (double)i);
            }

            System.IO.File.Delete("c:\\tmp\\data.parquet");
            await table.WriteAsync("c:\\tmp\\data.parquet");

            Table tbl = await Table.ReadAsync("c:\\tmp\\data.parquet");
            Assert.NotNull(tbl);
        }


        //[Fact]
        public async Task Write3() {
            var schema = new ParquetSchema(
                new DataField<DateTime>("Timestamp"),
                new DataField<string>("EventName"),
                new DataField<double>("MeterName"));

            var column1 = new DataColumn(
                (DataField)schema[0],
                Enumerable.Range(0, 1_000_000).Select(i => DateTime.UtcNow.AddSeconds(i)).ToArray());

            var column2 = new DataColumn(
                (DataField)schema[1],
                Enumerable.Range(0, 1_000_000).Select(i => i % 2 == 0 ? "on" : "off").ToArray());

            var column3 = new DataColumn(
                (DataField)schema[2],
                Enumerable.Range(0, 1_000_000).Select(i => (double)i).ToArray());

            System.IO.File.Delete("c:\\tmp\\data3.parquet");

            using(Stream fs = System.IO.File.OpenWrite("c:\\tmp\\data3.parquet")) {
                using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs)) {
                    using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                        await groupWriter.WriteColumnAsync(column1);
                        await groupWriter.WriteColumnAsync(column2);
                        await groupWriter.WriteColumnAsync(column3);
                    }
                }
            }

            using(Stream fs = System.IO.File.OpenRead("c:\\tmp\\data3.parquet")) {
                using(ParquetReader reader = await ParquetReader.CreateAsync(fs)) {
                    for(int i = 0; i < reader.RowGroupCount; i++) { 
                        using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i)) {
                            
                            foreach(DataField df in reader.Schema.GetDataFields()) {
                                DataColumn columnData = await rowGroupReader.ReadColumnAsync(df);

                                // do something to the column...
                            }

                        }
                    }
                }
            }
        }
    }

    class DocRef {
        public async Task ReadIntro() {
            // open file stream
            using(Stream fileStream = System.IO.File.OpenRead("c:\\test.parquet")) {
                // open parquet file reader
                using(ParquetReader parquetReader = await ParquetReader.CreateAsync(fileStream)) {
                    // get file schema (available straight after opening parquet reader)
                    // however, get only data fields as only they contain data values
                    DataField[] dataFields = parquetReader.Schema.GetDataFields();

                    // enumerate through row groups in this file
                    for(int i = 0; i < parquetReader.RowGroupCount; i++) {
                        // create row group reader
                        using(ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i)) {
                            // read all columns inside each row group (you have an option to read only
                            // required columns if you need to.
                            var columns = new DataColumn[dataFields.Length];
                            for(int c = 0; i < columns.Length; i++) {
                                columns[c] = await groupReader.ReadColumnAsync(dataFields[i]);
                            }

                            // get first column, for instance
                            DataColumn firstColumn = columns[0];

                            // .Data member contains a typed array of column data you can cast to the type of the column
                            Array data = firstColumn.Data;
                            int[] ids = (int[])data;
                        }
                    }
                }
            }
        }

        public async Task WriteIntro() {
            //create data columns with schema metadata and the data you need
            var idColumn = new DataColumn(
               new DataField<int>("id"),
               new int[] { 1, 2 });

            var cityColumn = new DataColumn(
               new DataField<string>("city"),
               new string[] { "London", "Derby" });

            // create file schema
            var schema = new ParquetSchema(idColumn.Field, cityColumn.Field);

            using(Stream fileStream = System.IO.File.OpenWrite("c:\\test.parquet")) {
                using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream)) {
                    parquetWriter.CompressionMethod = CompressionMethod.Gzip;
                    parquetWriter.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;
                    // create a new row group in the file
                    using(ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup()) {
                        await groupWriter.WriteColumnAsync(idColumn);
                        await groupWriter.WriteColumnAsync(cityColumn);
                    }
                }
            }
        }
    }
}