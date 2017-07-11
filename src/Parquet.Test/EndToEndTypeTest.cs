using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace Parquet.Test
{
   public class EndToEndTypeTest
   {
      [Theory]
      [InlineData("plain string")]
      [InlineData("L'Oréal Paris")]
      public void Strings(string input)
      {
         var ds = new DataSet(new SchemaElement<string>("s"));
         ds.Add(input);

         var ms = new MemoryStream();

         using (var writer = new ParquetWriter(ms))
         {
            writer.Write(ds, CompressionMethod.None);
         }

         DataSet ds1;
         ms.Position = 0;
         using (var reader = new ParquetReader(ms))
         {
            ds1 = reader.Read();
         }


         Assert.Equal(ds[0].GetString(0), ds1[0].GetString(0));
      }

      [Fact]
      public void Floats()
      {
         var ds = new DataSet(new SchemaElement<float>("f"));
         ds.Add((float)1.23);

         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         Assert.Equal(ds[0].GetFloat(0), ds1[0].GetFloat(0));
      }

      [Fact]
      public void Doubles()
      {
         var ds = new DataSet(new SchemaElement<double>("d"));
         ds.Add((double)12.34);

         var ms = new MemoryStream();
         ParquetWriter.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         Assert.Equal(ds[0].GetDouble(0), ds1[0].GetDouble(0));
      }

   }
}
