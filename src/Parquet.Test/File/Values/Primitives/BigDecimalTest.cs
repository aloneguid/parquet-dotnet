using Parquet.File.Values.Primitives;
using Xunit;

namespace Parquet.Test.File.Values.Primitives {
    public class BigDecimalTest {
        [Fact]
        public void Valid_but_massive_bigdecimal() {
            var bd = new BigDecimal(83086059037282.54m, 38, 16);

            //if exception is not thrown (overflow) we're OK
        }
    }
}
