using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;

namespace Parquet.Test.Xunit {
    public static class XAssert {

        private static readonly JsonSerializerOptions Options = new JsonSerializerOptions {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };

        public static void JsonEquivalent(object? expected, object? actual) {
            
            string expectedJson = JsonSerializer.Serialize(expected, Options);
            string actualJson = JsonSerializer.Serialize(actual, Options);

            Assert.Equal(expectedJson, actualJson);
        }

        public static void JsonEquivalent<T>(string? jsonLinesExpected, IEnumerable<T> actual) {
            string actualLines = string.Join(Environment.NewLine, actual.Select(d => JsonSerializer.Serialize(d, Options)));

            Assert.Equal(jsonLinesExpected, actualLines);
        }
    }
}
