using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.Extensions;
using Parquet.File;

namespace Parquet {
    /// <summary>
    /// Defines extension methods to simplify Parquet usage (experimental v3)
    /// </summary>
    public static class ParquetExtensions {
        /// <summary>
        /// Writes a file with a single row group
        /// </summary>
        public static async Task WriteSingleRowGroupParquetFileAsync(
            this Stream stream, Schema schema, params DataColumn[] columns) {
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
        public static async Task<(Schema, DataColumn[])> ReadSingleRowGroupParquetFile(this Stream stream) {
            Schema schema;
            DataColumn[] columns;
            using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
                schema = reader.Schema;

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
        /// Reads the first row group as a table
        /// </summary>
        /// <param name="reader">Open reader</param>
        /// <returns></returns>
        public static async Task<Table> ReadAsTableAsync(this ParquetReader reader) {
            Table result = null;

            if(reader.RowGroupCount == 0) {
                result = new Table(reader.Schema, null, 0);
            } else {
                for(int i = 0; i < reader.RowGroupCount; i++) {
                    using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i)) {
                        DataField[] dataFields = reader.Schema.GetDataFields();
                        DataColumn[] allData = new DataColumn[dataFields.Length];

                        for(int c = 0; c < dataFields.Length; c++) {
                            allData[c] = await rowGroupReader.ReadColumnAsync(dataFields[c]);
                        }

                        var t = new Table(reader.Schema, allData, rowGroupReader.RowCount);

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

            return result;
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
        /// Decodes raw bytes from <see cref="Thrift.Statistics"/> into a CLR value
        /// </summary>
        public static object DecodeSingleStatsValue(this Thrift.FileMetaData fileMeta, Thrift.ColumnChunk columnChunk, byte[] rawBytes) {
            if(rawBytes == null || rawBytes.Length == 0)
                return null;

            var footer = new ThriftFooter(fileMeta);
            Thrift.SchemaElement schema = footer.GetSchemaElement(columnChunk);

            IDataTypeHandler handler = DataTypeFactory.Match(schema, new ParquetOptions { TreatByteArrayAsString = true });

            using(var ms = new MemoryStream(rawBytes))
            using(var reader = new BinaryReader(ms)) {
                object value = handler.Read(reader, schema, rawBytes.Length);
                return value;
            }
        }
    }
}
