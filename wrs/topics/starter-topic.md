# About %product%

[![NuGet](https://img.shields.io/nuget/v/Parquet.Net.svg)](https://www.nuget.org/packages/Parquet.Net) [![Nuget](https://img.shields.io/nuget/dt/Parquet.Net)](https://www.nuget.org/packages/Parquet.Net)

![](banner.png)

%product% is a **fully managed, safe, extremely fast** .NET library to read and ✍write [Apache Parquet](https://parquet.apache.org/) files designed for .NET world (not a wrapper). Targets `.NET 7`, `.NET 6.0`, `.NET Core 3.1`,  `.NET Standard 2.1` and `.NET Standard 2.0`.

Whether you want to build apps for Linux, MacOS, Windows, iOS, Android, Tizen, Xbox, PS4, Raspberry Pi, Samsung TVs or much more, %product% has you covered.

## Why

Parquet is a great format for storing and processing large amounts of data, but it can be tricky to use with .NET. That's why this library is here to help. It's a pure library that doesn't need any external dependencies, and it's super fast - faster than Python and Java, and other C# solutions. It's also native to .NET, so you don't have to deal with any wrappers or adapters that might slow you down or limit your options.

This library is the best option for parquet files in .NET. It has a simple and intuitive API, supports all the parquet features you need, and handles complex scenarios with ease.

Also it:

- Has zero dependencies - pure library that just works.
- Really fast. Faster than Python and Java, and alternative C# implementations out there. It's often even faster than native C++ implementations.
- .NET native. Designed to utilise .NET and made for .NET developers, not the other way around.
- Not a "wrapper" that forces you to fit in. It's the other way around - forces parquet to fit into .NET.

## Quick start

Parquet is designed to handle *complex data in bulk*. It's *column-oriented* meaning that data is physically stored in columns rather than rows. This is very important for big data systems if you want to process only a subset of columns - reading just the right columns is extremely efficient.

As a quick start, suppose we have the following data records we'd like to save to parquet:

1. Timestamp.
2. Event name.
3. Meter value.

Or, to translate it to C# terms, this can be expressed as the following class:

<code-block lang="c#">
class Record {
    public DateTime Timestamp { get; set; }
    public string EventName { get; set; }
    public double MeterValue { get; set; }
}
</code-block>

### Writing data

Let's say you have around a million of events like that to save to a `.parquet` file. There are three ways to do that with this library, starting from easiest to hardest.

#### Writing with class serialisation

The first one is the easiest to work with, and the most straightforward. Let's generate those million fake records:

<code-block lang="c#">
var data = Enumerable.Range(0, 1_000_000).Select(i => new Record {
    Timestamp = DateTime.UtcNow.AddSeconds(i),
    EventName = i % 2 == 0 ? "on" : "off",
    MeterValue = i 
}).ToList();
</code-block>

Now, to write these to a file in say `/mnt/storage/data.parquet` you can use the following **line** of code:

```C#
await ParquetSerializer.SerializeAsync(data, "/mnt/storage/data.parquet");
```

That's pretty much it! You can [customise many things](serialisation.md) in addition to the magical magic process, but if you are a really lazy person that will do just fine for today.

#### Writing with row based API

Another way to serialise data is to use [row-based API](rows.md). They look at your data as a `Table`, which consists of a set of `Row`s. Basically looking at the data backwards from the point of view of how Parquet format sees it. However, that's how most people think about data. This is also useful when converting data from row-based formats to parquet and vice versa. Anyway, use it, I won't judge you (very often).

Let's generate a million of rows for our table, which is slightly more complicated. First, we need to declare table and it's schema:

```C#
var table = new Table(
    new DataField<DateTime>("Timestamp"),
    new DataField<string>("EventName"),
    new DataField<double>("MeterName"));
```

The code above says we are creating a new empty table with 3 *fields*, identical to example above with class serialisation. We are essentially declaring table's [schema](schema.md) here. Parquet format is *strongly typed* and all the rows will have to have *identical amount of values and their types*.

Now that empty table is ready, add a million rows to it:

```C#
for(int i = 0; i < 1_000_000; i++) {
    table.Add(
        DateTime.UtcNow.AddSeconds(1),
        i % 2 == 0 ? "on" : "off",
        (double)i);
}
```

The data will be identical to example above. And to write the table to a file:

```C#
await table.WriteAsync("/mnt/storage/data.parquet");
```

Of course this is a trivial example, and you can [customise it further](rows.md).

#### Writing with low level API

And finally, the lowest level API is the third method. This is the most performant, most Parquet-resembling way to work with data, but least intuitive and involves some knowledge of Parquet data structures.

First of all, you need schema. Always. Just like in row-based example, schema can be declared in the following way:

```C#
var schema = new ParquetSchema(
    new DataField<DateTime>("Timestamp"),
    new DataField<string>("EventName"),
    new DataField<double>("MeterName"));
```

Then, data columns need to be prepared for writing. As parquet is column-based format, low level APIs expect that low level column slice of data. I'll just shut up and show you the code:

```C#
var column1 = new DataColumn(
    schema.DataFields[0],
    Enumerable.Range(0, 1_000_000).Select(i => DateTime.UtcNow.AddSeconds(i)).ToArray());

var column2 = new DataColumn(
    schema.DataFields[1],
    Enumerable.Range(0, 1_000_000).Select(i => i % 2 == 0 ? "on" : "off").ToArray());

var column3 = new DataColumn(
    schema.DataFields[2],
    Enumerable.Range(0, 1_000_000).Select(i => (double)i).ToArray());
```

Important thing to note here - `columnX` variables represent data in an entire column, all the values in that column independently from other columns. Values in other columns have the same order as well. So we have created three columns with data identical to the two examples above.

Time to write it down:

```C#
using(Stream fs = System.IO.File.OpenWrite("/mnt/storage/data.parquet")) {
    using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs)) {
        using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
            
            await groupWriter.WriteColumnAsync(column1);
            await groupWriter.WriteColumnAsync(column2);
            await groupWriter.WriteColumnAsync(column3);
        }
    }
}
```

<procedure title="What's going on?">
<step>
We are creating output file stream. You can probably use one of the overloads in the next line though. This will be the receiver of parquet data. The stream needs to be writeable and seekable.
</step>
<step>
<code>ParquetWriter</code> is low-level class and is a root object to start writing from. It mostly performs coordination, check summing and enveloping of other data.
</step>
<step>
Row group is like a data partition inside the file. In this example we have just one, but you can create more if there are too many values that are hard to fit in computer memory.
</step>
<step>
Three calls to row group writer write out the columns. Note that those are performed sequentially, and in the same order as schema defines them.
</step>
</procedure>

Read more on writing [here](writing.md) which also includes guides on writing [nested types](nested_types.md) such as lists, maps, and structs.

### Reading data

Reading data also has three different approaches, so I'm going to unwrap them here in the same order as above.

#### Reading with class deserialisation

Provided that you have written the data, or just have some external data with the same structure as above, you can read those by simply doing the following:

```C#
IList<Record> data = await ParquetSerializer.DeserializeAsync<Record>("/mnt/storage/data.parquet");
```

This will give us an array with one million class instances similar to this:

![](read-classes.png)

Of course [class serialisation](serialisation.md) has more to it, and you can customise it further than that.

#### Reading with row based API

A read counterpart to the write example above is also a simple one-liner:

```C#
Table tbl = await Table.ReadAsync("/mnt/storage/data.parquet");
```

This will do the magic behind the scenes, give you table schema and rows, similar to this:

![](read-rows.png)

As always, there's [more to it](rows.md).

#### Reading with low level API

And with low level API the reading is even more flexible:

```C#
using(Stream fs = System.IO.File.OpenRead("/mnt/storage/data.parquet")) {
    using(ParquetReader reader = await ParquetReader.CreateAsync(fs)) {
        for(int i = 0; i < reader.RowGroupCount; i++) { 
            using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i)) {

                foreach(DataField df in reader.Schema.GetDataFields()) {
                    DataColumn columnData = await rowGroupReader.ReadColumnAsync(df);

                    // do something to the column...
                }
            }
        }
    }
}
```

<procedure title="This is what's happening">
<step>
Create read stream <code>fs</code>.
</step>
<step>
Create <code>ParquetReader</code> - root class for read operations.
</step>
<step>
The reader has <code>RowGroupCount</code> property which indicates how many row groups (like partitions) the file contains.
</step>
<step>
Explicitly open row group for reading.
</step>
<step>
Read each <code>DataField</code> from the row group, in the same order as it's declared in the schema.
</step>
</procedure>
 
> Uou can also use web based [reader app](https://aloneguid.github.io/parquet-online/) to test your files, which was created using this library!

## Choosing the API

If you have a choice, then the choice is easy - use Low Level API. They are the fastest and the most flexible. But what if you for some reason don't have a choice? Then think about this:

| Feature               | Class Serialisation | Table API        | Low Level API            |
|-----------------------|---------------------|------------------|--------------------------|
| Performance           | high                | very low         | very high                |
| Developer Convenience | C# native           | feels like Excel | close to Parquet internals |
| Row based access      | easy                | easy             | hard                     |
| Column based access   | C# native           | hard             | easy                     |

