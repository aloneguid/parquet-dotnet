using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test; 

public class CompressionTest : TestBase {
    [Theory]
    [InlineData(CompressionMethod.None)]
    [InlineData(CompressionMethod.Snappy)]
    [InlineData(CompressionMethod.Gzip)]
    //[InlineData(CompressionMethod.Lzo)]
    [InlineData(CompressionMethod.Brotli)]
    [InlineData(CompressionMethod.LZ4)]
    [InlineData(CompressionMethod.Zstd)]
    [InlineData(CompressionMethod.Lz4Raw)]
    public async Task All_compression_methods_supported_for_simple_integers(CompressionMethod compressionMethod) {
        const int value = 5;
        object actual = await WriteReadSingle(new DataField<int>("id"), value, compressionMethod);
        Assert.Equal(5, (int)actual);
    }

    [Theory]
    [InlineData(CompressionMethod.None)]
    [InlineData(CompressionMethod.Snappy)]
    [InlineData(CompressionMethod.Gzip)]
    //[InlineData(CompressionMethod.Lzo)]
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

    [Fact]
    public async Task Zstd_decompression_succeeds_when_parquet_metadata_has_incorrect_uncompressed_page_size() {
        // This file has a Zstd-compressed column whose UncompressedPageSize in the Parquet metadata
        // is incorrect (8 instead of the actual 1024 bytes). The library must ignore the metadata hint
        // and rely solely on the size embedded in the Zstd frame header.
        using ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile("special/zstd-invalid-length.parquet"));
        using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0);

        DataField[] fields = reader.Schema.GetDataFields();
        DataColumn column1 = await groupReader.ReadColumnAsync(fields[0]);
        DataColumn column2 = await groupReader.ReadColumnAsync(fields[1]);

        Assert.NotNull(column1.Data);
        Assert.NotNull(column2.Data);
    }
}