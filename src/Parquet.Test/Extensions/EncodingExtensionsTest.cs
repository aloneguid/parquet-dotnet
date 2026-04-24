using System;
using Parquet.Extensions;
using Xunit;

namespace Parquet.Test.Extensions;

public class EncodingExtensionsTest {

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void ZigZagLongTest(long number) {
        long enc = number.ZigZagEncode();
        long dec = enc.ZigZagDecode();

        Assert.Equal(number, dec);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void ULEB128Test(long number) {
        byte[] buf = new byte[10];
        int offset1 = 0;
        int offset2 = 0;
        ((ulong)number).ULEB128Encode(buf, ref offset1);
        ulong dec = ((Span<byte>)buf).ULEB128Decode(ref offset2);

        Assert.Equal(offset1, offset1);
        Assert.Equal(number, (long)dec);
    }
}