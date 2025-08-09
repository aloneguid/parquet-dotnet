using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
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
            // create file schema
            var schema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("city"));

            //create data columns with schema metadata and the data you need
            var idColumn = new DataColumn(
               schema.DataFields[0],
               new int[] { 1, 2 });

            var cityColumn = new DataColumn(
               schema.DataFields[1],
               new string[] { "London", "Derby" });

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

        public async Task AppendDemo() {
            //write a file with a single row group
            var schema = new ParquetSchema(new DataField<int>("id"));
            var ms = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(schema.DataFields[0], new int[] { 1, 2 }));
                }
            }

            //append to this file. Note that you cannot append to existing row group, therefore create a new one
            ms.Position = 0;    // this is to rewind our memory stream, no need to do it in real code.
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, append: true)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(schema.DataFields[0], new int[] { 3, 4 }));
                }
            }

            //check that this file now contains two row groups and all the data is valid
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(2, reader.RowGroupCount);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(2, rg.RowCount);
                    Assert.Equal(new int[] { 1, 2 }, (await rg.ReadColumnAsync(schema.DataFields[0])).Data);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(2, rg.RowCount);
                    Assert.Equal(new int[] { 3, 4 }, (await rg.ReadColumnAsync(schema.DataFields[0])).Data);
                }

            }
        }

        public async Task CustomMetadata() {
            var ms = new MemoryStream();
            var schema = new ParquetSchema(new DataField<int>("id"));

            //write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                writer.CustomMetadata = new Dictionary<string, string> {
                    ["key1"] = "value1",
                    ["key2"] = "value2"
                };

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(schema.DataFields[0], new[] { 1, 2, 3, 4 }));
                }
            }

            //read back
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal("value1", reader.CustomMetadata["key1"]);
                Assert.Equal("value2", reader.CustomMetadata["key2"]);
            }
        }
    }
}