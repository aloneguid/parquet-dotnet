using System.IO;
using System.IO.Compression;
using NetBox.Generator;
using Parquet.File.Streams;
using Parquet.Test.Xunit;
using Xunit;

namespace Parquet.Test
{
   public class SnappyStreamTest
   {
      [Theory]
      [Repeat(100)]
#pragma warning disable xUnit1026 // Theory methods should use all of their parameters
      public void Compress_decompress_random_byte_chunks(int index)
#pragma warning restore xUnit1026 // Theory methods should use all of their parameters
      {
         byte[] stage1 = RandomGenerator.GetRandomBytes(2, 1000);
         byte[] stage2;
         byte[] stage3;

         using (var source = new MemoryStream())
         {
            using (var snappy = new SnappyInMemoryStream(source, CompressionMode.Compress))
            {
               snappy.Write(stage1, 0, stage1.Length);
               snappy.MarkWriteFinished();
            }
            stage2 = source.ToArray();
         }

         using (var source = new MemoryStream(stage2))
         {
            using (var snappy = new SnappyInMemoryStream(source, CompressionMode.Decompress))
            {
               using (var ms = new MemoryStream())
               {
                  snappy.CopyTo(ms);
                  stage3 = ms.ToArray();
               }
            }
         }

         // validate

         Assert.Equal(stage1, stage3);

      }
   }
}
