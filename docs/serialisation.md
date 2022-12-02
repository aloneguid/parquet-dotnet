# Fast Automatic Serialisation

Parquet library is generally extremely flexible in terms of supporting internals of the Apache Parquet format and allows you to do whatever the low level API allow to. However, in many cases writing boilerplate code is not suitable if you are working with business objects and just want to serialise them into a parquet file. 

For this reason the library implements a fast serialiser/deserialiser for parquet files.

They both generate IL code in runtime for a class type to work with, therefore there is a tiny bit of a startup compilation delay which in most cases is negligible. Once the class is serialised at least once, further operations become blazingly fast. Note that this is still faster than reflection. In general, we see around *x40* speed improvement comparing to reflection on relatively large amounts of data (~5 million records).

Both serialiser and deserialiser works with array of classes. Let's say you have the following class definition:

```csharp
public class SimpleStructure
{
   public int Id { get; set; }

   public string Name { get; set; }
}
```

Let's generate a few instances of those for a test:

```csharp
SimpleStructure[] structures = Enumerable
   .Range(0, 1000)
   .Select(i => new SimpleStructure
   {
      Id = i,
      Name = $"row {i}",
   })
   .ToArray();
```

Here is what you can do to write out those classes in a single file:

```csharp
ParquetConvert.Serialize(structures, stream);
```

That's it! Of course the `.Serialize()` method also has overloads and optional parameters allowing you to control the serialization process slightly, such as selecting compression method, row group size etc.

Parquet.Net will automatically figure out file schema by reflecting class structure, types, nullability and other parameters for you.

In order to deserialise this file back to array of classes you would write the following:

```csharp
SimpleStructure[] structures = ParquetConvert.Deserialize<SimpleStructure>(stream);
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

You can customise serialisation process by attributing class properties with `[ParquetColumn]` attribute:

```csharp
class MyClass
{
   [ParquetColumn(Name = "cust_id")]
   public int Id { get; set; }
}
```

It supports the following properties:
- **Name** overrides column name in the parquet file.

### Ignoring properties while serializing

You can ignore few properties from serialization process by decorating them with `ParquetIgnore` attribute.

 with `[ParquetIgnore]` attribute:

```csharp
class MyClass
{
   [ParquetIgnore]
   public int Id { get; set; }
}
```

## Limitations

At the moment serialiser supports only simple first-level class *properties* (having a getter and a setter).  See the [inheritance docs](inheritance.md) for details on inherited properties.



None of the complex types such as arrays etc. are supported. We would love to hear your feedback whether this functionality is required though!
