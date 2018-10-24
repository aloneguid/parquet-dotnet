# Fast Automatic Serialisation

Parquet library is generally extremely flexible in terms of supporting internals of the Apache Parquet format and allows you to do whatever the low level API allow to. However, in many cases writing boilerplate code is not suitable if you are working with business objects and just want to serialise them into a parquet file. 

For this reason the library implements a fast serialiser/deserialiser for parquet files.

They both generate IL code in runtime for a class type to work with, therefore there is a tiny bit of a startup complation delay which in most cases is negligible. Once the class is serialised at least once, further operations become blazingly fast. Note that this is still faster than reflection. In general, we see around *x40* speed improvement comparing to reflection on relatively large amounts of data (~5 million records).

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

That's it! Of course `.Serialise()` method also has overloads and optional parameters allowing you to control the serialisation process slightly, such as selecting compression method, row group size etc.

Parquet.Net will automatically figure out file schema by reflecting class structure, types, nullability and other parameters for you.

In order to deserialise this file back to array of classes you would write the following:

```csharp
SimpleStructure[] structures = ParquetConvert.Deserialize<SimpleStructure>(stream);
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

## Limitations

At the moment serialiser supports only simple first-level class *properties* (having a getter and a setter).



None of the complex types such as arrays etc. are supported. We would love to hear your feedback whether this functionality is required though!
