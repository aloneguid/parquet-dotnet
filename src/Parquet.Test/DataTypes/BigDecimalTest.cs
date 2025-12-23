using Parquet.Data;
using Xunit;

namespace Parquet.Test.DataTypes; 

public class BigDecimalTest {
    [Fact]
    public void Valid_but_massive_bigdecimal() {
        var bd = BigDecimal.FromDecimal(83086059037282.54m, 38, 16);

        //if exception is not thrown (overflow) we're OK
    }

    [Fact]
    public void Bigger() {

    }
}
