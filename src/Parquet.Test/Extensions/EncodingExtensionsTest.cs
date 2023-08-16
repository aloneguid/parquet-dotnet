using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Extensions;
using Xunit;

namespace Parquet.Test.Extensions {
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
    }
}
