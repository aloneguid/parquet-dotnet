using System.IO;
using Parquet.Bloom;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Bloom;

public class Bloom_SpecCompliance_Test {
    [Fact]
    public void Serialized_Filter_RoundTrips_Block_XxHash_Uncompressed_Header() {
        var source = new SplitBlockBloomFilter(8);
        source.Insert(Encoding.UTF8.GetBytes("parquet"));
        var metadata = new ColumnMetaData();
        using var stream = new MemoryStream();

        BloomFilterIO.WriteToStream(
            stream,
            source,
            metadata,
            output => new ThriftCompactProtocolWriter(output));

        Assert.Equal(0, metadata.BloomFilterOffset);
        Assert.True(metadata.BloomFilterLength > source.NumberOfBlocks * 32);

        SplitBlockBloomFilter roundTrip = BloomFilterIO.ReadFromStream(
            stream,
            metadata,
            input => new ThriftCompactProtocolReader(input));

        Assert.True(roundTrip.MightContain(Encoding.UTF8.GetBytes("parquet")));
        Assert.False(roundTrip.MightContain(Encoding.UTF8.GetBytes("not-present")));
    }

    [Fact]
    public void Reader_Rejects_Invalid_NumBytes() {
        using var stream = new MemoryStream();
        var header = new BloomFilterHeader {
            NumBytes = 1,
            Algorithm = new BloomFilterAlgorithm { BLOCK = new SplitBlockAlgorithm() },
            Hash = new BloomFilterHash { XXHASH = new XxHash() },
            Compression = new BloomFilterCompression { UNCOMPRESSED = new Uncompressed() }
        };
        header.Write(new ThriftCompactProtocolWriter(stream));
        var metadata = new ColumnMetaData { BloomFilterOffset = 0 };

        Assert.Throws<InvalidDataException>(() => BloomFilterIO.ReadFromStream(
            stream,
            metadata,
            input => new ThriftCompactProtocolReader(input)));
    }
}
