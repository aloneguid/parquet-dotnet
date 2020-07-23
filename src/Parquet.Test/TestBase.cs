using System;
using System.IO;
using Parquet.Data;
using System.Linq;
using F = System.IO.File;
using Parquet.Data.Rows;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Parquet.Test
{
   public class TestBase
   {
      protected Stream OpenTestFile(string name)
      {
         return F.OpenRead("./data/" + name);
      }

      protected async Task<T[]> ConvertSerialiseDeserialiseAsync<T>(IEnumerable<T> instances) where T: new()
      {
         using (var ms = new MemoryStream())
         {
            Schema s = await ParquetConvert.SerializeAsync(instances, ms).ConfigureAwait(false);

            ms.Position = 0;

            return await ParquetConvert.DeserializeAsync<T>(ms).ConfigureAwait(false);
         }
      }

      protected async Task<Table> ReadTestFileAsTableAsync(string name)
      {
         using (Stream s = OpenTestFile(name))
         {
            await using (var reader = new ParquetReader(s))
            {
               return await reader.ReadAsTableAsync().ConfigureAwait(false);
            }
         }
      }

      protected async Task<Table> WriteReadAsync(Table table, bool saveLocal = false)
      {
         var ms = new MemoryStream();

         await using (var writer = new ParquetWriter(table.Schema, ms))
         {
            await writer.WriteAsync(table).ConfigureAwait(false);
         }

         if(saveLocal)
         {
            F.WriteAllBytes("c:\\tmp\\test.parquet", ms.ToArray());
         }

         ms.Position = 0;

         await using (var reader = new ParquetReader(ms))
         {
            return await reader.ReadAsTableAsync().ConfigureAwait(false);
         }
      }

      protected async Task<DataColumn> WriteReadSingleColumnAsync(DataField field, DataColumn dataColumn)
      {
         using (var ms = new MemoryStream())
         {
            // write with built-in extension method
            await ms.WriteSingleRowGroupParquetFileAsync(new Schema(field), dataColumn).ConfigureAwait(false);
            ms.Position = 0;

            //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

            // read first gow group and first column
            await using (var reader = new ParquetReader(ms))
            {
               if (reader.RowGroupCount == 0) return null;
               ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

               return await rgReader.ReadColumnAsync(field).ConfigureAwait(false);
            }


         }
      }

      protected async Task<(DataColumn[] Columns, Schema ReadSchema)> WriteReadSingleRowGroupAsync(Schema schema, DataColumn[] columns)
      {
         Schema readSchema;

         using (var ms = new MemoryStream())
         {
            await ms.WriteSingleRowGroupParquetFileAsync(schema, columns).ConfigureAwait(false);
            ms.Position = 0;

            //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

            await using (var reader = new ParquetReader(ms))
            {
               readSchema = reader.Schema;

               using (ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0))
               {
                  var columns2 = new DataColumn[columns.Length];

                  for (int i = 0; i < columns.Length; i++)
                  {
                     columns2[i] = await rgReader.ReadColumnAsync(columns[i].Field).ConfigureAwait(false);
                  }

                  return (columns2, readSchema);
               }
            }
         }
      }

      protected async Task<object> WriteReadSingleAsync(DataField field, object value, CompressionMethod compressionMethod = CompressionMethod.None, int compressionLevel = -1)
      {
         //for sanity, use disconnected streams
         byte[] data;

         using (var ms = new MemoryStream())
         {
            // write single value

            await using (var writer = new ParquetWriter(new Schema(field), ms))
            {
               writer.CompressionMethod = compressionMethod;
               writer.CompressionLevel = compressionLevel;

               using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
               {
                  Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                  dataArray.SetValue(value, 0);
                  var column = new DataColumn(field, dataArray);

                  await rg.WriteColumnAsync(column).ConfigureAwait(false);
               }
            }

            data = ms.ToArray();
         }

         using (var ms = new MemoryStream(data))
         { 
            // read back single value

            ms.Position = 0;
            await using (var reader = new ParquetReader(ms))
            {
               using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0))
               {
                  DataColumn column = await rowGroupReader.ReadColumnAsync(field).ConfigureAwait(false);

                  return column.Data.GetValue(0);
               }
            }
         }
      }
   }
}