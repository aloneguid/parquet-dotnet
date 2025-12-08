using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class CompressionTest : TestBase {
        [Theory]
        [InlineData(CompressionMethod.None)]
        [InlineData(CompressionMethod.Snappy)]
        [InlineData(CompressionMethod.Gzip)]
        [InlineData(CompressionMethod.Lzo)]
        [InlineData(CompressionMethod.Brotli)]
        [InlineData(CompressionMethod.LZ4)]
        [InlineData(CompressionMethod.Zstd)]
        [InlineData(CompressionMethod.Lz4Raw)]
        public async Task All_compression_methods_supported_for_simple_integeres(CompressionMethod compressionMethod) {
            const int value = 5;
            object actual = await WriteReadSingle(new DataField<int>("id"), value, compressionMethod);
            Assert.Equal(5, (int)actual);
        }

        [Theory]
        [InlineData(CompressionMethod.None)]
        [InlineData(CompressionMethod.Snappy)]
        [InlineData(CompressionMethod.Gzip)]
        [InlineData(CompressionMethod.Lzo)]
        [InlineData(CompressionMethod.Brotli)]
        [InlineData(CompressionMethod.LZ4)]
        [InlineData(CompressionMethod.Zstd)]
        [InlineData(CompressionMethod.Lz4Raw)]
        public async Task All_compression_methods_supported_for_simple_strings(CompressionMethod compressionMethod) {
            /*
             * uncompressed: length - 14, levels - 6
             * 
             * 
             */

            const string value = "five";
            object actual = await WriteReadSingle(new DataField<string>("id"), value, compressionMethod);
            Assert.Equal("five", actual);
        }
    }
}