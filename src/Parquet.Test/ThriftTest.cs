using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Test.Util;
using Xunit;

namespace Parquet.Test;

public class ThriftTest : TestBase {
    [Fact]
    public void TestFileRead_Table() {
        using Stream fs = OpenTestFile("thrift/wide.bin");
        FileMetaData fileMeta = FileMetaData.Read(new ThriftCompactProtocolReader(fs));
    }

    [Theory]
    [InlineData(PageType.DATA_PAGE)]
    [InlineData(PageType.DATA_PAGE_V2)]
    [InlineData(PageType.DICTIONARY_PAGE)]
    [InlineData(PageType.INDEX_PAGE)]
    public async Task Page_headers_roundtrip_using_only_async_reads(PageType type) {
        var statistics = new Statistics {
            Min = new byte[8192],
            Max = new byte[] { 1, 2, 3 },
            MinValue = Array.Empty<byte>(),
            MaxValue = new byte[] { 4, 5 },
            NullCount = long.MaxValue,
            DistinctCount = long.MinValue,
            IsMinValueExact = false,
            IsMaxValueExact = true,
            NanCount = 123
        };
        var expected = new PageHeader {
            Type = type,
            UncompressedPageSize = int.MaxValue,
            CompressedPageSize = 123,
            Crc = int.MinValue,
            DataPageHeader = type == PageType.DATA_PAGE ? new DataPageHeader {
                NumValues = 123,
                Encoding = Meta.Encoding.PLAIN,
                DefinitionLevelEncoding = Meta.Encoding.RLE,
                RepetitionLevelEncoding = Meta.Encoding.RLE,
                Statistics = statistics
            } : null,
            DataPageHeaderV2 = type == PageType.DATA_PAGE_V2 ? new DataPageHeaderV2 {
                NumValues = 123,
                NumNulls = 5,
                NumRows = 123,
                Encoding = Meta.Encoding.DELTA_BINARY_PACKED,
                DefinitionLevelsByteLength = 2,
                RepetitionLevelsByteLength = 3,
                IsCompressed = false,
                Statistics = statistics
            } : null,
            DictionaryPageHeader = type == PageType.DICTIONARY_PAGE ? new DictionaryPageHeader {
                NumValues = 123,
                Encoding = Meta.Encoding.PLAIN,
                IsSorted = false
            } : null,
            IndexPageHeader = type == PageType.INDEX_PAGE ? new IndexPageHeader() : null
        };
        using var data = new MemoryStream();
        expected.Write(new ThriftCompactProtocolWriter(data));
        byte[] expectedBytes = data.ToArray();
        data.WriteByte(42);
        data.Position = 0;
        using var source = new AsyncOnlyStream(data, 3);
        PageHeader actual = await PageHeader.ReadAsync(new ThriftCompactProtocolReader(source), CancellationToken.None);
        Assert.Equal(expectedBytes.Length, source.Position);
        using var result = new MemoryStream();
        actual.Write(new ThriftCompactProtocolWriter(result));
        Assert.Equal(expectedBytes, result.ToArray());
    }

    [Fact]
    public async Task Async_header_read_skips_unknown_nested_structs_and_lists() {
        using var data = new MemoryStream();
        var writer = new ThriftCompactProtocolWriter(data);
        writer.StructBegin();
        writer.BeginInlineStruct(100);
        writer.StructBegin();
        writer.WriteListBegin(1, (byte)CompactType.Struct, 16);
        for(int i = 0; i < 16; i++) {
            writer.StructBegin();
            writer.WriteBinaryField(1, new byte[] { 1, 2, 3 });
            writer.WriteBoolField(2, false);
            writer.StructEnd();
        }
        writer.WriteListBegin(2, (byte)CompactType.BooleanTrue, 2);
        writer.WriteBoolValue(true);
        writer.WriteBoolValue(false);
        writer.StructEnd();
        writer.WriteI32Field(1, (int)PageType.DATA_PAGE);
        writer.WriteI32Field(2, 123);
        writer.WriteI32Field(3, 45);
        writer.StructEnd();
        data.Position = 0;
        using var source = new AsyncOnlyStream(data, 1);

        PageHeader header = await PageHeader.ReadAsync(new ThriftCompactProtocolReader(source), CancellationToken.None);

        Assert.Equal(PageType.DATA_PAGE, header.Type);
        Assert.Equal(123, header.UncompressedPageSize);
        Assert.Equal(45, header.CompressedPageSize);
        Assert.Equal(data.Length, source.Position);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(8192)]
    public async Task Async_header_read_rejects_truncated_data(int length) {
        using var data = new MemoryStream();
        new PageHeader {
            Type = PageType.DATA_PAGE,
            DataPageHeader = new DataPageHeader {
                Statistics = new Statistics { MinValue = new byte[8192] }
            }
        }.Write(new ThriftCompactProtocolWriter(data));
        data.SetLength(length);
        data.Position = 0;
        using var source = new AsyncOnlyStream(data, 3);

        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await PageHeader.ReadAsync(new ThriftCompactProtocolReader(source), CancellationToken.None));
    }
}