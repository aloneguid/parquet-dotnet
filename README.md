# parquet-dotnet [![Build status](https://ci.appveyor.com/api/projects/status/w3o50mweytm85uxb?svg=true)](https://ci.appveyor.com/project/aloneguid/parquet-dotnet) [![NuGet](https://img.shields.io/nuget/v/Parquet.Net.svg)](https://www.nuget.org/packages/Parquet.Net)

![Icon](doc/img/icon.png)

A .NET library to read and write [Apache Parquet](https://github.com/Parquet) files. Supports .NET 4.5.1 and .NET Standard 1.6.

## Why

Parquet library is mostly available for Java, C++ and Python, which somewhat limits .NET/C# platform in big data applications. Whereas C# is a great language we still don't have anything good in this area.

This project is aimed to fix this problem.

## Roadmap

We have just started to work on this library, [contributors are welcome](CONTRIBUTING.md).

|Phase|Description|State|
|-----|-----------|-----|
|1|Implement reader which can understand the first test file (alltypes_plain.parquet). This is using a variety of encodings an no compression. Inline columns are not supported. Understand how to return results with minimum of boxing/unboxing. Support NULL values.|complete|
|2|Implement readers for any types not mentioned in phase 1. Implement writer for all types that reader supports. Publish alpha version on NuGet.|in progress|
|3|Support GZIP and SNAPPY decompression/compression|planning|
|4|Integrate with popular products like Azure Data Lakes|planning|

You can track the amount of features we have [implemented so far](doc/features.md).

## Getting started

**parquet-dotnet** is redistributed as a [NuGet package](https://www.nuget.org/packages/Parquet.Net) for `.NET 4.5.1` and `.NET Standard 1.6`. All code is managed and doesn't have any native dependencies, therefore you are ready to go after referencing the package.

### Reading files

In order to read a parquet file you need to open a stream first. Due to the fact that Parquet utilises file seeking extensively, the input stream must be *readable and seekable*. This somewhat limits the amount of streaming you can do, for instance you can't read a parquet file from a network stream as we need to jump around it, therefore you have to download it locally to disk and then open.

For instance, to read a file `c:\test.parquet` you woudl normally write the following code

```csharp
using System.IO;
using Parquet;

using(Stream fs = File.OpenRead("c:\\test.parquet"))
{
	using(var reader = new ParquetReader(fs))
	{
		ParquetDataSet ds = reader.Read();
	}
}
```

this will read entire file in memory as a set of columns inside `ParquetDataSet` class.

### Writing files

Parquet.Net operates on streams, therefore you need to create it first. The following example shows how to create a file on disk with two columns - `id` and `city`.

```csharp
using System.IO;
using Parquet;

var idColumn = new ParquetColumn<int>("id");
idColumn.Add(1, 2);

var cityColumn = new ParquetColumn<string>("city");
cityColumn.Add("London", "Derby");

var dataSet = new ParquetDataSet(idColumn, cityColumn);

using(Stream fileStream = File.OpenWrite("c:\\test.parquet"))
{
	using(var writer = new ParquetWriter(fileStream))
	{
		writer.Write(dataSet);
	}
}

```


## Tools

### parq

This tools gives a simple data inspector which lists out the columns found in a Parquet data set and the data values for those columns. 

To use, run ```dotnet parq.dll InputFilePath=path/to/file.parquet DisplayMinWidth=10```

![Parq](doc/img/parq.png)

## License

parquet-dotnet is licensed under the [MIT license](https://github.com/elastacloud/parquet-dotnet/blob/master/LICENSE).

## Contributing

All contributions are welcome. For details on how to start see [this guide](CONTRIBUTING.md). If you are a developer who is interested in Parquet development please [read this guide](doc/parquet-getting-started.md)
