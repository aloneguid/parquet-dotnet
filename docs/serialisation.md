# Class Serialisation

> for legacy serialisation refer to [this doc](legacy_serialisation.md).

Parquet library is generally extremely flexible in terms of supporting internals of the Apache Parquet format and allows you to do whatever the low level API allow to. However, in many cases writing boilerplate code is not suitable if you are working with business objects and just want to serialise them into a parquet file. 

Class serialisation is **really fast** as internally it generates [compiled expression trees](https://learn.microsoft.com/en-US/dotnet/csharp/programming-guide/concepts/expression-trees/) on the fly. That means there is a tiny bit of delay when serialising a first entity, which in most cases is negligible. Once the class is serialised at least once, further operations become blazingly fast (around *x40* speed improvement comparing to reflection on relatively large amounts of data (~5 million records)).

Class serialisation philosophy is trying to simply mimic .NET's built-in **json** serialisation infrastructure in order to ease in learning path and reuse as much existing code as possible.

## Quick Start

Both serialiser and deserialiser works with collection of classes. Let's say you have the following class definition:

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
await ParquetSerializer.SerializeAsync(data, "/mnt/storage/data.parquet");
```

That's it! Of course the `.SerializeAsync()` method also has overloads and optional parameters allowing you to control the serialization process slightly, such as selecting compression method, row group size etc.

Parquet.Net will automatically figure out file schema by reflecting class structure, types, nullability and other parameters for you.

In order to deserialise this file back to array of classes you would write the following:

```csharp
IList<Record> data = await ParquetSerializer.DeserializeAsync<Record>("/mnt/storage/data.parquet");
```
## Customising Serialisation

Serialisation tries to fit into C# ecosystem like a ninja 🥷, including customisations. It supports the following attributes from [`System.Text.Json.Serialization` Namespace](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization?view=net-7.0):

- [`JsonPropertyName`](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization.jsonpropertynameattribute?view=net-7.0) - changes mapping of column name to property name.
- [`JsonIgnore`](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization.jsonignoreattribute?view=net-7.0) - ignores property when reading or writing.

## Nested Types

You can also serialize [more complex types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types) supported by the Parquet format. If you would like to use low-level API for complex types, there is a [guide](nested_types.md) available too.

### Structures

Structures are just class members of a class and are completely transparent. For instance, `AddressBookEntry` class may contain a structure called `Address`:

```csharp
class Address {
    public string? Country { get; set; }

    public string? City { get; set; }
}

class AddressBookEntry {
    public string? FirstName { get; set; }

    public string? LastName { get; set; }   

    public Address? Address { get; set; }
}
```

Populated with the following fake data:

```csharp
var data = Enumerable.Range(0, 1_000_000).Select(i => new AddressBookEntry {
            FirstName = "Joe",
            LastName = "Bloggs",
            Address = new Address() {
                Country = "UK",
                City = "Unknown"
            }
        }).ToList();
```

You can serialise/deserialise those using the same `ParquetSerializer.SerializeAsync` / `ParquetSerializer.DeserializeAsync` methods. It does understand subclasses and will magically traverse inside them.

### Lists



### Maps (Dictionaries)



## FAQ

**Q.** Can I specify schema for serialisation/deserialisation.

**A.** No. Your class definition is the schema, so you don't need to supply it separately.
