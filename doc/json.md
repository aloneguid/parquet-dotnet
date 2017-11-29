# JSON Support

> This feature is in preview and documentation on it is not yet included in the official release.

Due to the fact JSON is a really popular format in big data, we thought we've obliged to support the path of importing/exporting data from/to JSON.

JSON support is implemented as a separate library [![NuGet](https://img.shields.io/nuget/v/Parquet.Net.Json.svg)](https://www.nuget.org/packages/Parquet.Net.Json) which you need to install in addition to the core library, and depends on the popular [Json.NET](https://www.newtonsoft.com/json) package. 

## Reading JSON

Parquet.Net.Json allows you to add `JObject` class instances to a new or existing `DataSet` in an easy way:

```csharp
var ds = new DataSet(schema);
var extractor = new JsonDataExtractor(schema);

foreach(JObject jsonDocument in documentCollection)
{
   extractor.AddRow(ds, jsonDocument);
}
```

This code snippet takes a collection of `JObject` which usually are JSON documents reads from somewhere else, and calls `AddRow` which internally converts the documents according to the `schema` into a parquet `Row` object.

Note that `schema` can be anything you want - the library will look for data in incoming documents which correspond to the schema, and ignore anything else.

### Inferring Schema

There is an easy way to infer schema from an existing `JObject` (or a collection of `JObject`s):

```csharp
IEnumerable<JObject> documents = ...;

var inferrer = new JsonSchemaInferring();
Schema schema = inferrer.InferSchema(documents);
```

This snippet effectively enumerates all the documents and works out the common schema for them, which will be a union of all the fields. You can use from 1 to unlimited amount of documents, depending on how much sample data you want to provide for schema inferring.

## Writing JSON

Parquet.Net does not support writing to JSON at the moment, however this is currently on our roadmap.