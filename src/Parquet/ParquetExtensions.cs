using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet {

    /// <summary>
    /// Progress callback
    /// </summary>
    public delegate Task TableReaderProgressCallback(int progress, string message);

    /// <summary>
    /// Defines extension methods to simplify Parquet usage
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
    }
}
