using System;
using System.IO;
using System.Reflection;
using Parquet.Data;
using Parquet.File;
using Parquet.Test.data;
using System.Linq;
using F = System.IO.File;

namespace Parquet.Test
{
   public class TestBase
   {
      protected Stream OpenTestFile(string name)
      {
         return ResourceReader.Open(name);
      }

      internal DataColumn WriteReadSingleColumn(DataField field, int rowCount, DataColumn dataColumn)
      {
         using (var ms = new MemoryStream())
         {
            // write with built-in extension method
            ms.WriteSingleRowGroup(new Schema(field), rowCount, dataColumn);
            ms.Position = 0;

            // read first gow group and first column
            using (var reader = new ParquetReader(ms))
            {
               if (reader.RowGroupCount == 0) return null;
               ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

               return rgReader.ReadColumn(field);
            }


         }
      }

      protected object WriteReadSingle(DataField field, object value, CompressionMethod compressionMethod = CompressionMethod.None)
      {
         //for sanity, use disconnected streams
         byte[] data;

         using (var ms = new MemoryStream())
         {
            // write single value

            using (var writer = new ParquetWriter(new Schema(field), ms))
            {
               writer.CompressionMethod = compressionMethod;

               using (ParquetRowGroupWriter rg = writer.CreateRowGroup(1))
               {
                  Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                  dataArray.SetValue(value, 0);
                  var column = new DataColumn(field, dataArray);

                  rg.Write(column);
               }
            }

            data = ms.ToArray();
         }

         using (var ms = new MemoryStream(data))
         { 
            // read back single value

            ms.Position = 0;
            using (var reader = new ParquetReader(ms))
            {
               using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0))
               {
                  DataColumn column = rowGroupReader.ReadColumn(field);

                  return column.Data.GetValue(0);
               }
            }
         }
      }
   }
}