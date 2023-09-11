using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Parquet.Test.Xunit {
    public static class XAssert {
        public static void JsonEquivalent(object? expected, object? actual) {
            string expectedJson = JsonSerializer.Serialize(expected);
            string actualJson = JsonSerializer.Serialize(actual);

            Assert.Equal(expectedJson, actualJson);
        }

        public static void JsonEquivalent<T>(string? jsonLinesExpected, IEnumerable<T> actual) {
            string actualLines = string.Join(Environment.NewLine, actual.Select(d => JsonSerializer.Serialize(d)));

            Assert.Equal(jsonLinesExpected, actualLines);
        }
    }
}
