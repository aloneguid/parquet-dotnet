# Untyped Serializer

Untyped serializer gives this library the ability to write and read data without the need to define low-level column data or define classes. This is extremely useful for use case like:

- Reading data from a file where the schema is not known in advance (i.e. parquet file viewers, generic utilities).
- Writing parquet file converters, i.e. from parquet to JSON.

<tip>
Since v5 this API supersedes the Row API, which was a very old and buggy API that was extremely hard to evolve. Untyped serializer is the future of dynamic data serialization in this library.
</tip>

and so on.

## Motivation
1. Single codebase for class serializer and untyped dictionary serializer.
2. De-serialization produces JSON-like structures in memory. These can be written back to JSON file as is.
3. Row API is an old legacy that is somewhat buggy and very hard to evolve and fix.

## Usage

In this API, everything is `Dictionary<string, object>`. For a simple use-case, with the following schema:

```C#
var schema = new ParquetSchema(
    new DataField<int>("id"),
    new DataField<string>("city"));
```

you can write data like this:

```C#
var data = new List<Dictionary<string, object>>
{
    new Dictionary<string, object> { { "id", 1 }, { "city", "London" } },
    new Dictionary<string, object> { { "id", 2 }, { "city", "Derby" } }
};

await ParquetSerializer.SerializeAsync(schema, data, stream);
```

For more examples, see `ParquetSerializerTests.cs` in the codebase. The documentation will evolve as this API gets more stable.