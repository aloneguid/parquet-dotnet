# Untyped Serializer

Version 4.19.0 introduced a new experimental feature: the ability to write and read data without the need to define low-level column data or define classes. This is extremely useful for use case like:

- Reading data from a file where the schema is not known in advance (i.e. parquet file viewers).
- Writing parquet file converters, i.e. from parquet to JSON.

and so on.

## Motivation
1. Single codebase for class serializer and untyped dictionary serializer.
2. De-serialization produces JSON-like structures in memory. These can be written back to JSON file as is.
3. Row API is an old legacy that is somewhat buggy and very hard to evolve and fix.

## Release plan
This will be released as an addition to the current class serializer in v4. Row API methods will be marked as deprecated and eventually removed in v5, making it a primary dynamic API.

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