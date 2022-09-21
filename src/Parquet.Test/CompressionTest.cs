using System.Threading.Tasks;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class CompressionTest : TestBase
   {
      [Theory]
      [InlineData(CompressionMethod.None)]
      [InlineData(CompressionMethod.Gzip)]
      [InlineData(CompressionMethod.Snappy)]
      public async Task All_compression_methods_supported_for_simple_integeres(CompressionMethod compressionMethod)
      {
         const int value = 5;
         object actual = await WriteReadSingle(new DataField<int>("id"), value, compressionMethod);
         Assert.Equal(5, (int)actual);
      }

      [Theory]
      [InlineData(CompressionMethod.None)]
      [InlineData(CompressionMethod.Gzip)]
      [InlineData(CompressionMethod.Snappy)]
      public async Task All_compression_methods_supported_for_simple_strings(CompressionMethod compressionMethod)
      {
         /*
          * uncompressed: length - 14, levels - 6
          * 
          * 
          */

         const string value = "five";
         object actual = await WriteReadSingle(new DataField<string>("id"), value, compressionMethod);
         Assert.Equal("five", actual);
      }

      [Theory]
      [InlineData(-1)]
      [InlineData(0)]
      [InlineData(1)]
      [InlineData(2)]
      public async Task Gzip_all_levels(int level)
      {
         const string value = "five";
         object actual = await WriteReadSingle(new DataField<string>("id"), value, CompressionMethod.Gzip, level);
         Assert.Equal("five", actual);
      }
   }
}
