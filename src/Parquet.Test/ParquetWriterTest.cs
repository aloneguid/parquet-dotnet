using System.IO;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test
{
   public class ParquetWriterTest
   {
      [Fact]
      public void Write_simple_int32_and_int_reads_back()
      {
         var ms = new MemoryStream();

         using (var writer = new ParquetWriter(ms))
         {
            var ds = new ParquetDataSet();

            //8 values for each column

            var idCol = new ParquetColumn<int>("id");
            idCol.Add(4, 5, 6, 7, 2, 3, 0, 1);

            var bool_col = new ParquetColumn<bool>("bool_col");
            bool_col.Add(true, false, true, false, true, false, true, false);

            var string_col = new ParquetColumn<string>("string_col");
            string_col.Add("0", "1", "0", "1", "0", "1", "0", "1");

            writer.Write(new ParquetDataSet(idCol, bool_col, string_col));
         }

         ms.Position = 0;
         using (var reader = new ParquetReader(ms))
         {
            ParquetDataSet ds = reader.Read();
         }

#if DEBUG
         const string path = "c:\\tmp\\first.parquet";
         F.WriteAllBytes(path, ms.ToArray());
#endif

      }
   }
}
