using System.IO;
using Xunit;

namespace Parquet.Test
{
   public class ParquetWriterTest
   {
      //[Fact]
      public void Write_simple_bool_and_int_reads_back()
      {
         using (var ms = new MemoryStream())
         {
            using (var writer = new ParquetWriter(ms))
            {
               var ds = new ParquetDataSet();

               var intCol = new ParquetColumn<int>("id");
               intCol.Add(1, 2, 3, 4, 5);

               writer.Write(new ParquetDataSet(intCol));
            }
         }
      }
   }
}
