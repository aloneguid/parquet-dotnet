# Class serialisation

Parquet.Net is generally extremely flexible in terms of supporting internals of the Apache Parquet format and allows you to do whatever the low level API allow to. However, in many cases writing boilerplate code is not suitable if you are working with business objects and just want to serialise them into a parquet file. 

Class serialisation is **really fast** as internally it generates [compiled expression trees](https://learn.microsoft.com/en-US/dotnet/csharp/programming-guide/concepts/expression-trees/) on the fly. That means there is a tiny bit of delay when serialising a first entity, which in most cases is negligible. Once the class is serialised at least once, further operations become amazingly fast (around *x40* speed improvement comparing to reflection on relatively large amounts of data (~5 million records)).

> [!TIP]
> Class serialisation philosophy is based on the idea that we don't need to reinvent the wheel when it comes to converting objects to and from JSON. Instead of creating our own custom serializers and deserializers, we can leverage the existing JSON infrastructure that .NET provides. This way, we can save time and effort, and also make our code more consistent and compatible with other .NET applications that use JSON.

## Quick start

Both serialiser and deserialiser work with collection of classes. Let's say you have the following class definition:

```C#
class Record {
    public DateTime Timestamp { get; set; }
    public string EventName { get; set; }
    public double MeterValue { get; set; }
}
```

Let's generate a few instances of those for a test:

```C#
var data = Enumerable.Range(0, 1_000_000).Select(i => new Record {
    Timestamp = DateTime.UtcNow.AddSeconds(i),
    EventName = i % 2 == 0 ? "on" : "off",
    MeterValue = i 
}).ToList();
```

Here is what you can do to write out those classes in a single file:

```C#
await ParquetSerializer.SerializeAsync(data, "/mnt/storage/data.parquet");
```

That's it! Of course the `.SerializeAsync()` method also has overloads and optional parameters allowing you to control the serialization process slightly, such as selecting compression method, row group size etc.

Parquet.Net will automatically figure out file schema by reflecting class structure, types, nullability and other parameters for you.

In order to deserialize this file back to array of classes you would write the following:

```C#
IList<Record> data = await ParquetSerializer.DeserializeAsync<Record>("/mnt/storage/data.parquet");
```

> [!NOTE]
> Target `Record` class can have more properties than the source file, and they will be gracefully skipped when deserializing.

## Deserialize records by `RowGroup`

If you have a large file, and you want to deserialize it in chunks, you can also read records by row group. This can help to keep memory usage low as you won't need to load the entire file into memory.

```C#
IList<Record> data = await ParquetSerializer.DeserializeAsync<Record>("/mnt/storage/data.parquet", rowGroupIndex);
```

## Class member requirements

Parquet.Net can serialize and deserialize into class properties and class fields (class fields support was introduced in _v4.23.0_).

Apparently, in order to serialize a class, a property must be readable, and in order to deserialize a class, a property must be writable. This is a standard requirement for any serialisation library.

In case of fields, they are by default both readable and writable, so you don't need to do anything special to make them work. 

## Enum support

Starting with version _4.24.0_, Parquet.Net supports [.NET enums](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/enum). These are stored in both schema and parquet file as their underlying type (by default, it's `System.Int32`).  

## Customising serialisation

Serialisation tries to fit into C# ecosystem like a ninja 🥷, including customisations. It supports the following attributes from [`System.Text.Json.Serialization` Namespace](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization?view=net-7.0):

- [`JsonPropertyName`](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization.jsonpropertynameattribute?view=net-7.0) - changes mapping of column name to property name. See also [ignoring property casing](#ignoring-property-casing).
- [`JsonIgnore`](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization.jsonignoreattribute?view=net-7.0) - ignores property when reading or writing.
- [`JsonPropertyOrder`](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.serialization.jsonpropertyorderattribute?view=net-6.0) - allows to reorder columns when writing to file (by default they are written in class definition order). Only root properties and struct (classes) properties can be ordered (it won't make sense to do the others).

Where built-in JSON attributes are not sufficient, extra attributes are added. Find extra attributes below in the relevant sections. List of generic attributes is presented below:

### Generic attributes

- `[ParquetIgnore]` - functionally equivalent to `JsonIgnore` attribute, use when your code is conflicting with `[JsonIgnore]` attribute. Other than that, there are no differences.

### Strings

In .NET, [`string`](https://learn.microsoft.com/en-us/dotnet/api/system.string?view=net-8.0) class is a reference type, which means it can be `null`. However, Parquet specification allows types to be declared _required or optional_. 

To fit into .NET ecosystem as closely as possible, this library will serialize .NET strings as _optional_ by default. If you want to change this behaviour, you can use `[ParquetRequired]` attribute:

```C#
public string OptionalString { get; set; }

[ParquetRequired]
public string RequiredString { get; set; }
```

In this example of three properties, `OptionalString`will be serialized as optional, but `RequiredString` will be serialized as required.

> [!NOTE]
> Parquet.Net will also expect string to be optional when deserializing from a file. If you have a required string in your file, you will get an exception until you add `[ParquetRequired]` attribute to the relevant class property.  

### Dates

By default, dates (`DateTime`) are serialized as `INT96` number, which include nanoseconds in the day. In general, `INT96` is obsolete in Parquet, however older systems such as Impala and Hive are still actively using it to represent dates.

Therefore, when this library sees `INT96` type, it will automatically treat it as a date for both serialization and deserialization.

If you need to rather use a normal non-legacy date type, just annotate a property with `[ParquetTimestamp]`:

```C#
[ParquetTimestamp]
public DateTime TimestampDate { get; set; }
```

which by default serialises date with millisecond precision. If you need to increase precision, you can use `[ParquetTimestamp]` attribute with an appropriate precision:

```C#
[ParquetTimestamp(ParquetTimestampResolution.Microseconds)]
public DateTime TimestampDate { get; set; }
```

> [!WARNING]
> Storing dates with microseconds precision relies on .NET `DateTime` type, which can only store microseconds starting from .NET 7. 

### Times

By default, time (`TimeSpan`) is serialised with millisecond precision. but you can increase it by adding `[ParquetMicroSecondsTime]` attribute:

```C#
[ParquetMicroSecondsTime]
public TimeSpan MicroTime { get; set; }
```

### Decimals Numbers

By default, `decimal` is serialized with precision (number of digits in a number) of `38` and scale (number of digits to the right of the decimal point in a number) of `18`. If you need to use different precision/scale pair, use `[ParquetDecimal]` attribute:

```C#
[ParquetDecimal(40, 20)]
public decimal With_40_20 { get; set; }
```

### Legacy Repeatable (Legacy Arrays)

One of the features of Parquet files is that they can contain simple repeatable fields, also known as arrays, that can store multiple values for a single column. However, this feature is not widely supported by most of the systems that process Parquet files, and it may cause errors or compatibility issues. An example of such a file can be found in test data folder, called `legacy_primitives_collection_arrays.parquet`. 

If you want to read an array of primitive values, such as integers or booleans, from a parquet file created by another system, you might think that you can simply use a list property in your class, like this:

```C#
class Primitives {
        public List<bool>? Booleans { get; set; }
}

IList<Primitives> data = await ParquetSerializer.DeserializeAsync<Primitives>(input);
```

However, this will not work, because this library expects a list of complex objects, not a list of primitives. It will throw an exception when it encounters an array in the parquet file.

To fix this problem, you need to use the `ParquetSimpleRepeatable` attribute on your list property. This tells the library that the list contains simple values that can be repeated as an array. For example:

```C#
class Primitives {
        [ParquetSimpleRepeatable]
        public List<bool>? Booleans { get; set; }
}
```

This will successfully deserialize the array of integers from the parquet file into your list property.

## Nested types

You can also serialize [more complex types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types) supported by the Parquet format. Sometimes you might want to store more complex data in your parquet files, like lists or maps. These are called *nested types* and they can be useful for organizing your information. However, they also come with a trade-off: they make your code slower and use more CPU resources. That's why you should only use them when you really need them and not just because they look cool. Simple columns are faster and easier to work with, so stick to them whenever you can.

> If you would like to use low-level API for complex types, there is a [guide](nested_types.md) available too.

### Structures

Structures are just class members of a class and are completely transparent. For instance, `AddressBookEntry` class may contain a structure called `Address`:

```C#
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

```C#
var data = Enumerable.Range(0, 1_000_000).Select(i => new AddressBookEntry {
            FirstName = "Joe",
            LastName = "Bloggs",
            Address = new Address() {
                Country = "UK",
                City = "Unknown"
            }
        }).ToList();
```

You can serialise/deserialize those using the same `ParquetSerializer.SerializeAsync` / `ParquetSerializer.DeserializeAsync` methods. It does understand subclasses and will magically traverse inside them.

### Lists

One of the cool things about lists is that Parquet can handle any kind of data structure in a list. You can have a list of atoms, like `1, 2, 3`, or a list of lists, `[[1, 2], [3, 4], [5, 6]]`, or even a list of structures. Parquet.Net is awesome like that!

For instance, a simple `MovementHistory` class with `Id` and list of `ParentIds` looking like the following:

```C#
class MovementHistoryCompressed  {
    public int? PersonId { get; set; }

    public List<int>? ParentIds { get; set; }
}
```

Is totally fine to serialise/deserialise:

```C#
var data = Enumerable.Range(0, 100).Select(i => new MovementHistoryCompressed {
    PersonId = i,
    ParentIds = Enumerable.Range(i, 4).ToList()
}).ToList();

await ParquetSerializer.SerializeAsync(data, "c:\\tmp\\lat.parquet");
```



 Reading it in `Spark` produces the following schema

```
root
 |-- PersonId: integer (nullable = true)
 |-- ParentIds: array (nullable = true)
 |    |-- element: integer (containsNull = true)
```

and data:

```
+--------+---------------+
|PersonId|ParentIds      |
+--------+---------------+
|0       |[0, 1, 2, 3]   |
|1       |[1, 2, 3, 4]   |
|2       |[2, 3, 4, 5]   |
|3       |[3, 4, 5, 6]   |
|4       |[4, 5, 6, 7]   |
|5       |[5, 6, 7, 8]   |
|6       |[6, 7, 8, 9]   |
|7       |[7, 8, 9, 10]  |
|8       |[8, 9, 10, 11] |
|9       |[9, 10, 11, 12]|
+--------+---------------+
```

Or as a more complicate example, here is a list of structures (classes in C#):

```C#
class Address {
    public string? Country { get; set; }

    public string? City { get; set; }
}

class MovementHistory {
    public int? PersonId { get; set; }

    public string? Comments { get; set; }

    public List<Address>? Addresses { get; set; }
}

 var data = Enumerable.Range(0, 1_000).Select(i => new MovementHistory {
    PersonId = i,
    Comments = i % 2 == 0 ? "none" : null,
    Addresses = Enumerable.Range(0, 4).Select(a => new Address {
        City = "Birmingham",
        Country = "United Kingdom"
    }).ToList()
}).ToList();

await ParquetSerializer.SerializeAsync(data, "c:\\tmp\\ls.parquet");
```

that by reading from Spark produced the following schema

```
root
 |-- PersonId: integer (nullable = true)
 |-- Comments: string (nullable = true)
 |-- Addresses: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Country: string (nullable = true)
 |    |    |-- City: string (nullable = true)
```

and data

```
+--------+--------+--------------------+
|PersonId|Comments|           Addresses|
+--------+--------+--------------------+
|       0|    none|[{United Kingdom,...|
|       1|    null|[{United Kingdom,...|
|       2|    none|[{United Kingdom,...|
|       3|    null|[{United Kingdom,...|
|       4|    none|[{United Kingdom,...|
|       5|    null|[{United Kingdom,...|
|       6|    none|[{United Kingdom,...|
|       7|    null|[{United Kingdom,...|
|       8|    none|[{United Kingdom,...|
|       9|    null|[{United Kingdom,...|
+--------+--------+--------------------+
```

#### Nullability and lists

By default, Parquet.Net assumes that both lists and list elements are nullable. This is because list is a class in .NET, and classes are nullable. Therefore, coming back to the previous example definition:

```c#
class MovementHistory {
    public List<Address>? Addresses { get; set; }
}
```

will produce the following [low-level schema](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists):

```
OPTIONAL group Addresses (LIST) {
  repeated group list {
    OPTIONAL group element {
        OPTIONAL binary Country (UTF8);
        OPTIONAL binary City (UTF8);
    }
  }
}
```

Usually this is not a problem, however you might encounter a problem when deserialising some files to handcrafted C# classes when nullability of the files and classes do not match the default.

To fix this, you can use `[ParquetRequired]` attribute on the list property:

```c#
class MovementHistory {
    [ParquetRequired]
    public List<Address>? Addresses { get; set; }
}
```

which will in turn produce the following low-level schema:

```
REQUIRED group Addresses (LIST) {
  repeated group list {
    OPTIONAL group element {
        OPTIONAL binary Country (UTF8);
        OPTIONAL binary City (UTF8);
    }
  }
}
```

As you can see, "Addresses" container is now "required". If you also need to mark the actual element ("Address" instance) as nullable, you can use `[ParquetListElementRequired]` attribute on the element property:

```c#
class MovementHistory {
    [ParquetRequired, ParquetListElementRequired]
    public List<Address>? Addresses { get; set; }
}
```

which will produce the following low-level schema:

```
REQUIRED group Addresses (LIST) {
  repeated group list {
    REQUIRED group element {
        OPTIONAL binary Country (UTF8);
        OPTIONAL binary City (UTF8);
    }
  }
}
```

### Maps (Dictionaries)

Maps are useful constructs if you need to serialize key-value pairs where each row can have different amount of keys.

For example, if you want to store partition values like so:

```json
{"partition1": "value1", "partition2": "value2" }
```

without knowing beforehand how many partitions there will be.

In this library, maps are represented as an instance of generic `IDictionary<TKey, TValue>` type. 

To give you a minimal example, let's say we have the following class with two properties: `Id` and `Tags`. The `Id` property is an integer that can be used to identify a row or an item in a collection. The `Tags` property is a dictionary of strings that can store arbitrary key-value pairs. For example, the `Tags` property can be used to store metadata or attributes of the item:

```C#
class IdWithTags {
    public int Id { get; set; }

    public Dictionary<string, string>? Tags { get; set; }
}
```

You can easily use `ParquetSerializer` to work with this class:

```C#
var data = Enumerable.Range(0, 10).Select(i => new IdWithTags { 
    Id = i,
    Tags = new Dictionary<string, string> {
        ["id"] = i.ToString(),
        ["gen"] = DateTime.UtcNow.ToString()
    }}).ToList();

await ParquetSerializer.SerializeAsync(data, "c:\\tmp\\map.parquet");
```

When read by Spark, the schema looks like the following:

```
root
 |-- Id: integer (nullable = true)
 |-- Tags: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)

```

And the data:

```
+---+-------------------------------------+
|Id |Tags                                 |
+---+-------------------------------------+
|0  |{id -> 0, gen -> 17/03/2023 13:06:04}|
|1  |{id -> 1, gen -> 17/03/2023 13:06:04}|
|2  |{id -> 2, gen -> 17/03/2023 13:06:04}|
|3  |{id -> 3, gen -> 17/03/2023 13:06:04}|
|4  |{id -> 4, gen -> 17/03/2023 13:06:04}|
|5  |{id -> 5, gen -> 17/03/2023 13:06:04}|
|6  |{id -> 6, gen -> 17/03/2023 13:06:04}|
|7  |{id -> 7, gen -> 17/03/2023 13:06:04}|
|8  |{id -> 8, gen -> 17/03/2023 13:06:04}|
|9  |{id -> 9, gen -> 17/03/2023 13:06:04}|
+---+-------------------------------------+
```



### Supported collection types

Similar to JSON [supported collection types](https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/supported-collection-types?pivots=dotnet-7-0), here are collections Parquet.Net currently supports:

| Type                                                         | Serialization | Deserialization |
| ------------------------------------------------------------ | ------------- | --------------- |
| [Single-dimensional array](https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/arrays/single-dimensional-arrays) `**` | ✔️             | ❌               |
| [Multi-dimensional arrays](https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/arrays/multidimensional-arrays) `*` | ❌             | ❌               |
| [`IList<T>`](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ilist-1?view=net-7.0) | ✔️             | ❌`**`           |
| [`List<T>`](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ilist-1?view=net-7.0) | ✔️             | ✔️               |
| [`IDictionary<TKey, TValue>`](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.idictionary-2?view=net-7.0) `**` | ❌             | ❌               |
| [`Dictionary<TKey, TValue>`](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.dictionary-2?view=net-7.0) | ✔️             | ✔️               |

`*` Technically impossible or very hard to implement.
`**` Technically possible, but not implemented yet.

## Appending to files

`ParquetSerializer` supports appending data to an existing Parquet file. This can be useful when you have multiple batches of data that need to be written to the same file.

To use this feature, you need to set the `Append` flag to `true` in the `ParquetSerializerOptions` object that you pass to the `SerializeAsync` method. This will tell the library to append the data batch to the end of the file stream instead of overwriting it. For example:

```C#
await ParquetSerializer.SerializeAsync(dataBatch, ms, new ParquetSerializerOptions { Append = true });
```

However, there is one caveat: you should not set the `Append` flag to `true` for the first batch of data that you write to a new file. This is because a Parquet file has a header and a footer that contain metadata about the schema and statistics of the data. If you try to append data to an empty file stream, you will get an `IOException` because there is no header or footer to read from. Therefore, you should always set the `Append` flag to `false` for the first batch (or not pass any options, which makes it `false` by default) and then switch it to `true` for subsequent batches. For example:

```C#
// First batch
await ParquetSerializer.SerializeAsync(dataBatch1, ms, new ParquetSerializerOptions { Append = false });

// Second batch
await ParquetSerializer.SerializeAsync(dataBatch2, ms, new ParquetSerializerOptions { Append = true });

// Third batch
await ParquetSerializer.SerializeAsync(dataBatch3, ms, new ParquetSerializerOptions { Append = true });
```

By following this pattern, you can easily append data to a Parquet file using `ParquetSerializer`.

## Specifying row group size

Row groups are a logical division of data in a parquet file. They allow efficient filtering and scanning of data based on predicates. By default, all the class instances are serialized into a single row group, which is absolutely fine. If you need to set a custom row group size, you can specify it in `ParquetSerializerOptions` like so:

```C#
await ParquetSerializer.SerializeAsync(data, stream, new ParquetSerializerOptions { RowGroupSize = 10_000_000 });
```

Note that small row groups make parquet files very inefficient in general, so you should use this parameter only when you are absolutely sure what you are doing. For example, if you have a very large dataset that needs to be split into smaller files for distributed processing, you might want to use a smaller row group size to avoid having too many rows in one file. However, this will also increase the file size and the metadata overhead, so you should balance the trade-offs carefully.

## Ignoring property casing

Since v5.0 Parquet.Net supports ignoring property casing when deserializing data. This can be useful when you have a class with properties that have different casing than the column names in the parquet file. For example, if you have a class with properties like `FirstName` and `LastName`, but the parquet file has columns named `first_name` and `last_name`, you can use the `IgnorePropertyCasing` option to ignore the casing differences and match the properties with the columns. Here is an example:

```C#
class BeforeRename {
  public string? lowerCase { get; set; }
}

class AfterRename {
  public string? LowerCase { get; set; }
}

var data = Enumerable.Range(0, 1_000).Select(i => new BeforeRename {
    lowerCase = i % 2 == 0 ? "on" : "off"
}).ToList();

// serialise to memory stream
using var ms = new MemoryStream();
await ParquetSerializer.SerializeAsync(data, ms);
ms.Position = 0;

// this will deserialize the data, but `LowerCase` property will be null, because it does not exist in the parquet file.
IList<AfterRename> data2 = await ParquetSerializer.DeserializeAsync<AfterRename>(ms);

// this will successfully deserialize the data, because property names are case insensitive
IList<AfterRename> data2 = await ParquetSerializer.DeserializeAsync<AfterRename>(ms,
    new ParquetSerializerOptions { PropertyNameCaseInsensitive = true });
```

## FAQ

**Q.** Can I specify schema for serialisation/deserialization.

**A.** If you're using a class-based approach to define your data model, you don't have to worry about providing a schema separately. The class definition itself is the schema, meaning it specifies the fields and types of your data. This makes it easier to write and maintain your code, since you only have to define your data model once and use it everywhere.
