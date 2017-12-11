using System;
using System.IO;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class SimplestTest
   {
      [Fact]
      public void Run_perfect_expressive_boolean_column()
      {
         var schema = new Schema(new DataField("id", DataType.Boolean, false, false));
         var ds = new DataSet(schema);

         ds.Add(true);
         ds.Add(false);
         ds.Add(true);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

      }

      [Fact]
      public void Simple()
      {
         DataSet ds = new DataSet(new DataField<string>("City"), new DataField<Int32>("ID"));
         ds.Add("London", 1);
         ds.Add("Cochin", 2);
         using (Stream fileStream = System.IO.File.OpenWrite("c:\\tmp\\Demo.txt"))
         {
            using (var writer = new ParquetWriter(fileStream))
            {
               writer.Write(ds);
            }
         }

         DataSet dsread;
         using (Stream fs = System.IO.File.OpenRead("c:\\tmp\\Demo.txt"))
         {
            using (var reader = new ParquetReader(fs))
            {
               dsread = reader.Read();
            }
         }

      }
   }
}