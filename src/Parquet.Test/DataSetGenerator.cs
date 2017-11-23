using Parquet.Data;
using System.IO;

namespace Parquet.Test
{
   static class DataSetGenerator
   {
      public static DataSet WriteRead(this DataSet ds)
      {
         return WriteRead(ds, null);
      }

      public static string WriteReadFirstRow(this DataSet ds)
      {
         DataSet ds1 = WriteRead(ds, null);
         return ds1[0].ToString();
      }

      public static DataSet Generate(int rowCount)
      {
         var ds = new DataSet(new DataField<int>("id"));
         for(int i = 0; i < rowCount; i++)
         {
            var row = new Row(i);
            ds.Add(row);
         }
         return ds;
      }

      public static DataSet WriteRead(DataSet original, WriterOptions writerOptions = null)
      {
         var ms = new MemoryStream();

         ParquetWriter.Write(original, ms, CompressionMethod.None, null, writerOptions);

         ms.Position = 0;
         return ParquetReader.Read(ms);
      }
   }
}
