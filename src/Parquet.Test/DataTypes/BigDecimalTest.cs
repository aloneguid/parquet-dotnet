using System.Numerics;
using Parquet.Data;
using Xunit;

namespace Parquet.Test.DataTypes; 

public class BigDecimalTest {
    [Fact]
    public void Parse_from_decimal() {
        var bd = BigDecimal.FromDecimal(83086059037282.54m, 38, 16);
        Assert.Equal("83086059037282.5400000000000000", bd.ToString());
    }

    [Fact]
    public void Parse_from_BigInteger() {
        var bigInt = BigInteger.Parse("12345678901234567890123456789012345678");
        var bd = new BigDecimal(bigInt, 38, 2);
        Assert.Equal("123456789012345678901234567890123456.78", bd.ToString());
    }
}
