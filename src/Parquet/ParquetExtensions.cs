using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Defines extension methods to simplify Parquet usage (experimental v3)
   /// </summary>
   public static class ParquetExtensions
   {
      /// <summary>
      /// Writes a file with a single row group
      /// </summary>
      public static async Task WriteSingleRowGroupParquetFileAsync(this Stream stream, Schema schema, params DataColumn[] columns)
      {
         await using (var writer = new ParquetWriter(schema, stream))
         {
            writer.CompressionMethod = CompressionMethod.None;
            using (ParquetRowGroupWriter rgw = writer.CreateRowGroup())
            {
               foreach(DataColumn column in columns)
               {
                  await rgw.WriteColumnAsync(column).ConfigureAwait(false);
               }
            }
         }
      }

      /// <summary>
      /// Reads the first row group from a file
      /// </summary>
      /// <param name="stream"></param>
      public static async Task<(Schema Schema, DataColumn[] Columns)> ReadSingleRowGroupParquetFileAsync(this Stream stream) //TODO changed the signature because async methods cannot have out params, need to validate
      {
         await using (var reader = new ParquetReader(stream))
         {
            Schema schema = reader.Schema;

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               DataField[] dataFields = schema.GetDataFields();
               var columns = new DataColumn[dataFields.Length];

               for(int i = 0; i < dataFields.Length; i++)
               {
                  columns[i] = await rgr.ReadColumnAsync(dataFields[i]).ConfigureAwait(false);
               }

               return (schema, columns);
            }
         }
      }

      /// <summary>
      /// Writes entire table in a single row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static async Task WriteAsync(this ParquetWriter writer, Table table)
      {
         using (ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup())
         {
            await rowGroupWriter.WriteAsync(table).ConfigureAwait(false);
         }
      }

      /// <summary>
      /// Reads the first row group as a table
      /// </summary>
      /// <param name="reader">Open reader</param>
      /// <returns></returns>
      public static async Task<Table> ReadAsTableAsync(this ParquetReader reader)
      {
         Table result = null;

         for(int i = 0; i < reader.RowGroupCount; i++)
         {
            using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i))
            {
               DataField[] temp = reader.Schema.GetDataFields();

               DataColumn[] allData = new DataColumn[temp.Length];
               
               for (int j = 0; j < temp.Length; j++)
               {
                  allData[j] = await rowGroupReader.ReadColumnAsync(temp[j]).ConfigureAwait(false);
               }

               var t = new Table(reader.Schema, allData, rowGroupReader.RowCount);

               if(result == null)
               {
                  result = t;
               }
               else
               {
                  foreach(Row row in t)
                  {
                     result.Add(row);
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
      public static async Task WriteAsync(this ParquetRowGroupWriter writer, Table table)
      {
         foreach (DataColumn dc in table.ExtractDataColumns())
         {
            await writer.WriteColumnAsync(dc).ConfigureAwait(false);
         }
      }

      /// <summary>
      /// Decodes raw bytes from <see cref="Thrift.Statistics"/> into a CLR value
      /// </summary>
      public static object DecodeSingleStatsValue(this Thrift.FileMetaData fileMeta, Thrift.ColumnChunk columnChunk, byte[] rawBytes)
      {
         if (rawBytes == null || rawBytes.Length == 0) return null;

         var footer = new ThriftFooter(fileMeta);
         Thrift.SchemaElement schema = footer.GetSchemaElement(columnChunk);

         IDataTypeHandler handler = DataTypeFactory.Match(schema, new ParquetOptions { TreatByteArrayAsString = true });

         using (var ms = new MemoryStream(rawBytes))
         using (var reader = new BinaryReader(ms))
         {
            object value = handler.Read(reader, schema, rawBytes.Length);
            return value;
         }
      }
   }
}