using System;
using Xunit;

namespace Parquet.Test.Extensions {
    public class SpanExtensionsTest {

        [Fact]
        public void StringMinMax() {
            ReadOnlySpan<string> span = new string[] { "one", "two", "three" }.AsSpan();
            
            span.MinMax(out string? min, out string? max);

            Assert.Equal("one", min);
            Assert.Equal("two", max);
        }
    }
}
