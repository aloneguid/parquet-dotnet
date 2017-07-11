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
   }
}
