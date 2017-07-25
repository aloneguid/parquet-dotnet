# Parquet.Net [![Build status](https://ci.appveyor.com/api/projects/status/w3o50mweytm85uxb/branch/master?svg=true)](https://ci.appveyor.com/project/aloneguid/parquet-dotnet/branch/master) [![NuGet](https://img.shields.io/nuget/v/Parquet.Net.svg)](https://www.nuget.org/packages/Parquet.Net)

![Icon](doc/img/dotnetlovesparquet.png)

A .NET library to read and write [Apache Parquet](https://github.com/Parquet) files. Supports `.NET 4.5.1`, `.NET Standard 1.4` and `.NET Standard 1.6`.

## Why

Parquet library is mostly available for Java, C++ and Python, which somewhat limits .NET/C# platform in big data applications. Whereas C# is a great language we still don't have anything good in this area.

This project is aimed to fix this problem.

## Index

- [Supported features](doc/features.md)
- Programming guide
  - [Reading Data](doc/reading.md) 
  - [Writing Data](doc/writing.md)
  - [Working with DataSets](doc/dataset.md) 

## Roadmap

This library is almost ready for production use and [contributors are welcome](CONTRIBUTING.md).

|Phase|Description|State|
|-----|-----------|-----|
|1|Implement reader which can understand the first test file (alltypes_plain.parquet). This is using a variety of encodings an no compression. Inline columns are not supported. Understand how to return results with minimum of boxing/unboxing. Support NULL values.|complete|
|2|Implement readers for any types not mentioned in phase 1. Implement writer for all types that reader supports. Support GZIP and SNAPPY  compression/decompression. Migrate to row-based data model. Publish first stable version on NuGet.|nearly there|

You can track the amount of features we have [implemented so far](doc/features.md).

## Related Projects

- [Azure Data Lake Analytics extractor](https://github.com/elastacloud/datalake-extractor-parquet)
- [UWP Client for Windows 10](https://github.com/elastacloud/parquet-uwp)

## Getting started

**Parquet.Net** is redistributed as a [NuGet package](https://www.nuget.org/packages/Parquet.Net). All code is managed and doesn't have any native dependencies, therefore you are ready to go after referencing the package.

### Reading files

In order to read a parquet file you need to open a stream first. Due to the fact that Parquet utilises file seeking extensively, the input stream must be *readable and seekable*. This somewhat limits the amount of streaming you can do, for instance you can't read a parquet file from a network stream as we need to jump around it, therefore you have to download it locally to disk and then open.

For instance, to read a file `c:\test.parquet` you woudl normally write the following code

```csharp
using System.IO;
using Parquet;
using Parquet.Data;

using(Stream fs = File.OpenRead("c:\\test.parquet"))
{
	using(var reader = new ParquetReader(fs))
	{
		DataSet ds = reader.Read();
	}
}
```

this will read entire file in memory as a set of rows inside `DataSet` class.

### Writing files

Parquet.Net operates on streams, therefore you need to create it first. The following example shows how to create a file on disk with two columns - `id` and `city`.

```csharp
using System.IO;
using Parquet;
using Parquet.Data;

var ds = new DataSet(
	new SchemaElement<int>("id"),
	new SchemaElement<string>("city")
);

ds.Add(1, "London");
ds.Add(2, "Derby");

using(Stream fileStream = File.OpenWrite("c:\\test.parquet"))
{
	using(var writer = new ParquetWriter(fileStream))
	{
		writer.Write(ds);
	}
}

```


## Tools

### parq

Parq is a .NET runtime (written for Windows but will run elsewhere) that brings tooling for inspecting Parquet files to developers. It is a command line utility, for which you can find out more by [reading this guide](doc/parq.md).

## License

Parquet.Net is licensed under the [MIT license](https://github.com/elastacloud/parquet-dotnet/blob/master/LICENSE).

## Contributing

All contributions are welcome. For details on how to start see [this guide](CONTRIBUTING.md). If you are a developer who is interested in Parquet development please [read this guide](doc/parquet-getting-started.md)
