using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace Parquet.Test
{
   public class CompressionTest
   {
      [Fact]
      public void I_can_write_in_gzip_and_read_back()
      {
         var ms = new MemoryStream();
         DataSet ds1 = new DataSet(new SchemaElement<int>("id"));
         DataSet ds2;
         ds1.Add(5);

         //write
         using (var writer = new ParquetWriter(ms))
         {
            writer.Write(ds1, CompressionMethod.Gzip);
         }

         //read back
         using (var reader = new ParquetReader(ms))
         {
            ms.Position = 0;
            ds2 = reader.Read();
         }


         Assert.Equal(5, ds2[0].GetInt(0));
      }

      [Fact]
      public void I_can_write_snappy_and_read_back()
      {
         var ms = new MemoryStream();
         var ds1 = new DataSet(
            new SchemaElement<int>("id"),
            new SchemaElement<int>("no"));

         ds1.Add(1, 3);
         ds1.Add(2, 4);

         DataSet ds2;

         //write
         using (var writer = new ParquetWriter(ms))
         {
            writer.Write(ds1, CompressionMethod.Snappy);
         }

         //read back
         using (var reader = new ParquetReader(ms))
         {
            ms.Position = 0;
            ds2 = reader.Read();
         }

         Assert.Equal(1, ds2[0].GetInt(0));
         Assert.Equal(2, ds2[1].GetInt(0));
         Assert.Equal(3, ds2[0].GetInt(1));
         Assert.Equal(4, ds2[1].GetInt(1));
      }
   }
}
