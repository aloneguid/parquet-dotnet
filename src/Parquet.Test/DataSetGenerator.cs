using Parquet.Data;
using System.IO;

namespace Parquet.Test
{
   static class DataSetGenerator
   {
      public static DataSet Generate(int rowCount)
      {
         var ds = new DataSet(new SchemaElement<int>("id"));
         for(int i = 0; i < rowCount; i++)
         {
            var row = new Row(i);
            ds.Add(row);
         }
         return ds;
      }

      public static DataSet WriteRead(DataSet original)
      {
         var ms = new MemoryStream();

         ParquetWriter.Write(original, ms);

         ms.Position = 0;
         return ParquetReader.Read(ms);
      }
   }
}
