using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Analysis;
using Parquet.Data;
using Parquet.Data.Analysis;
using Parquet.Rows;
using Parquet.Schema;

namespace Parquet {

    /// <summary>
    /// Progress callback
    /// </summary>
    public delegate Task TableReaderProgressCallback(int progress, string message);

    /// <summary>
    /// Defines extension methods to simplify Parquet usage (experimental v3)
    /// </summary>
    public static class ParquetExtensions {
        /// <summary>
        /// Writes a file with a single row group
        /// </summary>
        public static async Task WriteSingleRowGroupParquetFileAsync(
            this Stream stream, ParquetSchema schema, params DataColumn[] columns) {
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream)) {
                writer.CompressionMethod = CompressionMethod.None;
                using(ParquetRowGroupWriter rgw = writer.CreateRowGroup()) {
                    foreach(DataColumn column in columns) {
                        await rgw.WriteColumnAsync(column);
                    }
                }
            }
        }

        /// <summary>
        /// Reads the first row group from a file
        /// </summary>
        /// <param name="stream"></param>
        public static async Task<(ParquetSchema, DataColumn[])> ReadSingleRowGroupParquetFile(this Stream stream) {
            ParquetSchema schema;
            DataColumn[] columns;
            using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
                schema = reader.Schema!;

                using(ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0)) {
                    DataField[] dataFields = schema.GetDataFields();
                    columns = new DataColumn[dataFields.Length];

                    for(int i = 0; i < dataFields.Length; i++) {
                        columns[i] = await rgr.ReadColumnAsync(dataFields[i]);
                    }
                }
            }
            return (schema, columns);
        }

        /// <summary>
        /// Writes entire table in a single row group
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="table"></param>
        public static async Task WriteAsync(this ParquetWriter writer, Table table) {
            using(ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup()) {
                await rowGroupWriter.WriteAsync(table);
            }
        }

        /// <summary>
        /// Writes table to a stream
        /// </summary>
        public static async Task WriteAsync(this Table table, Stream output,
            ParquetOptions? formatOptions = null, bool append = false, CancellationToken cancellationToken = default) {
            using ParquetWriter writer = await ParquetWriter.CreateAsync(table.Schema, output, formatOptions, append, cancellationToken);
            using(ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup()) {
                await rowGroupWriter.WriteAsync(table);
            }
        }

        /// <summary>
        /// Writes table to a file
        /// </summary>
        public static async Task WriteAsync(this Table table, string path,
            ParquetOptions? formatOptions = null, bool append = false, CancellationToken cancellationToken = default) {
            using Stream output = System.IO.File.OpenWrite(path);
            await WriteAsync(table, output, formatOptions, append, cancellationToken);
        }

        /// <summary>
        /// Reads all row groups as a table. Can be extremely slow.
        /// </summary>
        /// <param name="reader">Open reader</param>
        /// <param name="progressCallback"></param>
        /// <returns></returns>
        public static async Task<Table> ReadAsTableAsync(this ParquetReader reader,
            TableReaderProgressCallback? progressCallback = null) {
            Table? result = null;
            DataField[] dataFields = reader.Schema!.GetDataFields();

            int stepsTotal = dataFields.Length * reader.RowGroupCount;
            int currentStep = 0;

            if(reader.RowGroupCount == 0) {
                result = new Table(reader.Schema, null, 0);
            } else {
                for(int i = 0; i < reader.RowGroupCount; i++) {
                    using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i)) {
                        DataColumn[] allData = new DataColumn[dataFields.Length];

                        for(int c = 0; c < dataFields.Length; c++) {
                            if(progressCallback != null) {
                                await progressCallback(
                                    (int)(currentStep++ * 100 / (double)stepsTotal), 
                                    $"reading column '{dataFields[c].Name}' in row group #{i}");
                            }
                            allData[c] = await rowGroupReader.ReadColumnAsync(dataFields[c]);
                        }

                        var t = new Table(reader.Schema, allData, rowGroupReader.RowCount);
                        if(progressCallback != null) {
                            await progressCallback(
                                (int)(currentStep * 100 / (double)stepsTotal),
                                $"adding extra {t.Count} row(s) to result...");
                        }

                        if(result == null) {
                            result = t;
                        } else {
                            foreach(Row row in t) {
                                result.Add(row);
                            }
                        }
                    }
                }
            }

            if(progressCallback != null)
                await progressCallback(100, "done.");

            return result!;
        }

        /// <summary>
        /// Writes table to this row group
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="table"></param>
        public static async Task WriteAsync(this ParquetRowGroupWriter writer, Table table) {
            foreach(DataColumn dc in table.ExtractDataColumns()) {
                await writer.WriteColumnAsync(dc);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="inputStream"></param>
        /// <returns></returns>
        public static async Task<DataFrame> ReadParquetStreamAsDataFrameAsync(this Stream inputStream) {
            using ParquetReader reader = await ParquetReader.CreateAsync(inputStream);

            var dfcs = new List<DataFrameColumn>();
            for(int i = 0; i < reader.RowGroupCount; i++) {
                using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(i);

                foreach(DataField df in reader.Schema.DataFields.Where(df => df.MaxRepetitionLevel == 0)) {
                    DataColumn dc = await rgr.ReadColumnAsync(df);
                    DataFrameColumn dfc = DataFrameMapper.ToDataFrameColumn(dc);
                    dfcs.Add(dfc);
                }
            }

            return new DataFrame(dfcs);
        }
    }
}
