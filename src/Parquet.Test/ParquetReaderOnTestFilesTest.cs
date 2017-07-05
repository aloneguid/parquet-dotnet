using Parquet.Data;
using System;
using System.IO;
using System.Reflection;
using Xunit;

namespace Parquet.Test
{
   using F = System.IO.File;

   /// <summary>
   /// Tests a set of predefined test files that they read back correct
   /// </summary>
   public class ParquetReaderOnTestFilesTest
   {
      [Fact]
      public void FixedLenByteArray_dictionary()
      {
         using (Stream s = F.OpenRead(GetDataFilePath("fixedlenbytearray.parquet")))
         {
            using (var r = new ParquetReader(s))
            {
               DataSet ds = r.Read();
            }
         }
      }

      [Fact]
      public void Datetypes_all()
      {
         using (Stream s = F.OpenRead(GetDataFilePath("dates.parquet")))
         {
            using (var r = new ParquetReader(s))
            {
               DataSet ds = r.Read();
            }
         }
      }

      //[Fact]
      public void Delete_me_manual_test()
      {
         var ds = ParquetReader.ReadFile("C:\\tmp\\postcodes.plain.parquet");
      }

      private string GetDataFilePath(string name)
      {
         string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }
   }
}