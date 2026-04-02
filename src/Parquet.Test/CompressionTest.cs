using System.IO;
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
    public async Task Compression_roundtrip(CompressionMethod compressionMethod) {
        /*
         * uncompressed: length - 14, levels - 6
         * 
         * 
         */

        using var ms = new MemoryStream();
        var schema = new ParquetSchema(new DataField<int>("id"));

        int testValue = 10;

        // write value
        await using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms)) {
            w.CompressionMethod = compressionMethod;
            using ParquetRowGroupWriter rgw = w.CreateRowGroup();
            await rgw.WriteAsync<int>(schema.DataFields[0], new[] { testValue });
        }

        // read value back
        ms.Position = 0;
        await using(ParquetReader r = await ParquetReader.CreateAsync(ms)) {
            RawColumnData<int> c = await ReadColumn<int>(r, schema.DataFields[0]);
            Assert.Equal(testValue, c.Values[0]);
        }
    }

    [Fact]
    public async Task Zstd_decompression_succeeds_when_parquet_metadata_has_incorrect_uncompressed_page_size() {
        // This file has a Zstd-compressed column whose UncompressedPageSize in the Parquet metadata
        // is incorrect (8 instead of the actual 1024 bytes). The library must ignore the metadata hint
        // and rely solely on the size embedded in the Zstd frame header.
        await using ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile("special/zstd-invalid-length.parquet"));
        using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0);

        DataField[] fields = reader.Schema.GetDataFields();
        RawColumnData<int> column1 = await ReadColumn<int>(reader, fields[0]);
        RawColumnData<int> column2 = await ReadColumn<int>(reader, fields[1]);

        Assert.True(column1.Values.Length > 0);
        Assert.True(column2.Values.Length > 0);
    }
}