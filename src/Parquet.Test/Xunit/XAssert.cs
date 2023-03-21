using System.Text.Json;
using Xunit;

namespace Parquet.Test.Xunit {
    public static class XAssert {
        public static void JsonEquivalent(object? expected, object? actual) {
            string expectedJson = JsonSerializer.Serialize(expected);
            string actualJson = JsonSerializer.Serialize(actual);

            Assert.Equal(expectedJson, actualJson);
        }
    }
}
