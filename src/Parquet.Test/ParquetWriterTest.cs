using System.IO;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test
{
   public class ParquetWriterTest
   {
      //[Fact]
      public void Write_simple_int32_and_int_reads_back()
      {
         const string path = "c:\\tmp\\first.parquet";
         if (F.Exists(path)) F.Delete(path);

         using (var ms = F.OpenWrite(path))
         {
            using (var writer = new ParquetWriter(ms))
            {
               var ds = new ParquetDataSet();

               var intCol = new ParquetColumn<int>("id");
               intCol.Add(1, 2, 3, 4, 5);

               writer.Write(new ParquetDataSet(intCol));
            }
         }

         using (var ms = F.OpenRead(path))
         {
            using (var reader = new ParquetReader(ms))
            {
               ParquetDataSet ds = reader.Read();
            }
         }
      }
   }
}
