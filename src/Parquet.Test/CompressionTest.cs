using Parquet.Data;
using System.IO;
using Xunit;

namespace Parquet.Test
{
   public class CompressionTest : TestBase
   {
      [Theory]
      [InlineData(CompressionMethod.None)]
      [InlineData(CompressionMethod.Gzip)]
      [InlineData(CompressionMethod.Snappy)]
      public void All_compression_methods_supported(CompressionMethod compressionMethod)
      {
         //v2
         var ms = new MemoryStream();
         DataSet ds1 = new DataSet(new DataField<int>("id"));
         DataSet ds2;
         ds1.Add(5);

         //write
         using (var writer = new ParquetWriter(ms))
         {
            writer.Write(ds1, compressionMethod);
         }

         //read back
         using (var reader = new ParquetReader(ms))
         {
            ms.Position = 0;
            ds2 = reader.Read();
         }

         Assert.Equal(5, ds2[0].GetInt(0));

         //v3
         //looks like writing is not working in certain scenarios!
         //broken length: 177
         //correct length: 187
         const int value = 5;
         object actual = WriteReadSingle(new DataField<int>("id"), value, compressionMethod);
         Assert.Equal(5, (int)actual);
      }
   }
}
