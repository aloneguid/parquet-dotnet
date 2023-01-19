﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Test {
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