using System;
using System.Globalization;
using Xunit;

namespace Parquet.Test.Extensions {
    public class OtherExtensionsTest {

        [Theory]
        [InlineData("1969-12-31T23:59:59.0001Z", -1000)]
        [InlineData("1970-01-01T00:00:00.0001Z", 0)]
        [InlineData("9999-12-31T23:59:59.999Z", 253402300799999)]
        public void ToUnixMilliseconds_truncates_milliseconds_correctly(string dateString, long expectedMilliseconds) {
            var dateTime = DateTime.Parse(dateString, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);

            long actualMilliseconds = dateTime.ToUnixMilliseconds();

            Assert.Equal(expectedMilliseconds, actualMilliseconds);
        }
    }
}
