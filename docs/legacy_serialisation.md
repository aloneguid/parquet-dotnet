# Class Serialisation

> This document refers to legacy serialisation, which is still in the library, but is marked as obsolete and will be removed by the end of 2023. No new features will be added and you should [migrate](serialisation.md).

Parquet library is generally extremely flexible in terms of supporting internals of the Apache Parquet format and allows you to do whatever the low level API allow to. However, in many cases writing boilerplate code is not suitable if you are working with business objects and just want to serialise them into a parquet file. 

Class serialisation is **really fast** as it generates [MSIL](https://en.wikipedia.org/wiki/Common_Intermediate_Language) on the fly. That means there is a tiny bit of delay when serialising a first entity, which in most cases is negligible. Once the class is serialised at least once, further operations become blazingly fast (around *x40* speed improvement comparing to reflection on relatively large amounts of data (~5 million records)).

> At the moment class serialisation supports only simple first-level class *properties* (having a getter and a setter). None of the complex types such as arrays etc. are supported. This is mostly due to lack of time rather than technical limitations.

## Quick Start

Both serialiser and deserialiser works with array of classes. Let's say you have the following class definition:

```csharp
class Record {
    public DateTime Timestamp { get; set; }
    public string EventName { get; set; }
    public double MeterValue { get; set; }
}
```

Let's generate a few instances of those for a test:

```csharp
var data = Enumerable.Range(0, 1_000_000).Select(i => new Record {
    Timestamp = DateTime.UtcNow.AddSeconds(i),
    EventName = i % 2 == 0 ? "on" : "off",
    MeterValue = i 
}).ToList();
```

Here is what you can do to write out those classes in a single file:

```csharp
await ParquetConvert.SerializeAsync(data, "/mnt/storage/data.parquet");
```

That's it! Of course the `.SerializeAsync()` method also has overloads and optional parameters allowing you to control the serialization process slightly, such as selecting compression method, row group size etc.

Parquet.Net will automatically figure out file schema by reflecting class structure, types, nullability and other parameters for you.

In order to deserialise this file back to array of classes you would write the following:

```csharp
Record[] data = await ParquetConvert.DeserializeAsync<Record>("/mnt/storage/data.parquet");
```
### Retrieve and Deserialize records by RowGroup:

If you have a huge parquet file(~10million records), you can also retrieve records by rowgroup index (which could help to keep low memory footprint as you don't load everything into memory).
```csharp
SimpleStructure[] structures = ParquetConvert.Deserialize<SimpleStructure>(stream,rowGroupIndex);
```
### Deserialize only few properties:

If you have a parquet file with huge number of columns and you only need few columns for processing, you can retrieve required columns only as described in the below code snippet.
```csharp
class MyClass
{
   public int Id { get; set; }
   public string Name{get;set;}
   public string Address{get;set;}
   public int Age{get;set;}
}
class MyClassV1
{
   public string Name { get; set; }
}
SimpleStructure[] structures = Enumerable
   .Range(0, 1000)
   .Select(i => new SimpleStructure
   {
      Id = i,
      Name = $"row {i}",
   })
   .ToArray();
ParquetConvert.Serialize(structures, stream);

MyClassV1[] v1structures = ParquetConvert.Deserialize<MyClassV1>(stream,rowGroupIndex);
```

## Customising Serialisation

Serialisation tries to fit into C# ecosystem like a ninja 🥷, including customisations. It supports the following attributes from [`System.Text.Json.Serialization` Namespace](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization?view=net-7.0):

- [`JsonPropertyName`](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization.jsonpropertynameattribute?view=net-7.0) - changes mapping of column name to property name.
- [`JsonIgnore`](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization.jsonignoreattribute?view=net-7.0) - ignores property when reading or writing.
