using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;

namespace Parquet.Test.Xunit;

public static class XAssert {

    public class ReadOnlyMemoryConverter : JsonConverter<ReadOnlyMemory<char>> {
        public override ReadOnlyMemory<char> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => reader.TokenType == JsonTokenType.Null ? null : reader.GetString().AsMemory();

        public override void Write(Utf8JsonWriter writer, ReadOnlyMemory<char> value, JsonSerializerOptions options) {
            writer.WriteStringValue(value.Span);
        }

        public override ReadOnlyMemory<char> ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => reader.GetString().AsMemory();

        public override void WriteAsPropertyName(Utf8JsonWriter writer, ReadOnlyMemory<char> value, JsonSerializerOptions options) {
            writer.WritePropertyName(value.Span);
        }
    }

    private static readonly JsonSerializerOptions Options = new JsonSerializerOptions {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    static XAssert() {
        Options.Converters.Add(new ReadOnlyMemoryConverter());
    }

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
