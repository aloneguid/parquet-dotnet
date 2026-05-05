using System.IO;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings;

public class PlainEncoderTest {
    [Fact]
    public void EncodePlainBoolsTest() {
        bool[] data = new bool[] { true, false, true, true, false };
        var ms = new MemoryStream();
        ParquetPlainEncoder.Encode(data, ms);
        byte[] encoded = ms.ToArray();
        Assert.Single(encoded);
        Assert.Equal(13, encoded[0]);
    }

    /// <summary>
    /// This will be hardware accelerated
    /// </summary>
    [Fact]
    public void EncodeLargeBoolSpan() {
        bool[] data = new bool[1_000_000];
        for(int i = 0; i < data.Length; i++) {
            data[i] = (i % 2 == 0);
        }
        var ms = new MemoryStream();
        ParquetPlainEncoder.Encode(data, ms);
        byte[] encoded = ms.ToArray();
        Assert.Equal(125000, encoded.Length);
    }
}
