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

      internal DataColumn WriteReadSingleColumn(DataField field, int rowCount, DataColumn dataColumn, bool flushToDisk = false)
      {
         using (var ms = new MemoryStream())
         {
            // write with built-in extension method
            ms.WriteSingleRowGroup(new Schema(field), rowCount, dataColumn);
            ms.Position = 0;

            if(flushToDisk)
            {
               FlushTempFile(ms);
            }

            // read first gow group and first column
            using (var reader = new ParquetReader3(ms))
            {
               ParquetRowGroupReader rgReader = reader.FirstOrDefault();
               if (rgReader == null) return null;

               return rgReader.ReadColumn(field);
            }


         }
      }

      protected void FlushTempFile(MemoryStream ms)
      {
         F.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());
      }

      protected object WriteReadSingle(DataField field, object value)
      {
         using (var ms = new MemoryStream())
         {
            // write single value

            using (var writer = new ParquetWriter3(new Schema(field), ms))
            {
               writer.CompressionMethod = CompressionMethod.None;

               using (ParquetRowGroupWriter rg = writer.CreateRowGroup(1))
               {
                  var column = new DataColumn(field);
                  column.Add(value);

                  rg.Write(column);
               }
            }

            // read back single value

            ms.Position = 0;
            using (var reader = new ParquetReader3(ms))
            {
               foreach(ParquetRowGroupReader rowGroupReader in reader)
               {
                  DataColumn column = rowGroupReader.ReadColumn(field);

                  return column.DefinedData.OfType<object>().FirstOrDefault();
               }

               return null;
            }
         }
      }
   }
}