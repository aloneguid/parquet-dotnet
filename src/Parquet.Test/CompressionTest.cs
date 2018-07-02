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
      public void All_compression_methods_supported(CompressionMethod compressionMethod)
      {
         const int value = 5;
         object actual = WriteReadSingle(new DataField<int>("id"), value, compressionMethod);
         Assert.Equal(5, (int)actual);
      }
   }
}
