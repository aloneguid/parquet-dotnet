using System;
using System.IO;
using System.Reflection;
using Parquet.Data;
using Parquet.File;
using Parquet.Test.data;

namespace Parquet.Test
{
   public class TestBase
   {
      protected Stream OpenTestFile(string name)
      {
         return ResourceReader.Open(name);
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

            throw new NotImplementedException();
         }
      }
   }
}