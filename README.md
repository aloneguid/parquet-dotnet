# Apache Parquet for .Net Platform

![Icon](doc/img/dotnetlovesparquet.png)

> This version is a complete redesign of Parquet library and is not compatible with V2. To see documentation for the previous major version please [follow this link](https://github.com/elastacloud/parquet-dotnet/tree/final-v2)

## Status

[![NuGet](https://img.shields.io/nuget/v/Parquet.Net.svg)](https://www.nuget.org/packages/Parquet.Net)
[![Build status](https://ci.appveyor.com/api/projects/status/w3o50mweytm85uxb?svg=true)](https://ci.appveyor.com/project/aloneguid/parquet-dotnet)

**Fully managed** .NET library to read and write [Apache Parquet](https://parquet.apache.org/) files. Supports:
- `.NET 4.5` and up.
- `.NET Standard 1.4` and up (for those who are in a tank that means it supports `.NET Core` (all versions) implicitly)

Runs on all flavors of Windows, Linux, and mobile devices (iOS, Android) via [Xamarin](https://www.xamarin.com/)

## Why

Parquet library is mostly available for [Java](https://github.com/apache/parquet-mr), [C++](https://github.com/apache/parquet-cpp) and [Python](https://github.com/dask/fastparquet), which somewhat limits .NET/C# platform in big data applications. Whereas C# is a beautiful language (C# is just Java done right) working on all platforms and devices, we still don't have anything good in this area.

This project is aimed to fix this problem. We support all the popular server and client operating systems, mobile devices, [gaming consoles](doc/xboxone.md) and everything that can run `.NET` which is quite a lot!

## Index

- [Reading Data](doc/reading.md) 
- [Writing Data](doc/writing.md)
- [DataSet](doc/dataset.md) 
- [Declaring Schema](doc/schema.md)
  - [Supported Types](doc/types.md)
- Advanced Types
  - [Structures](doc/complex-struct.md)
  - [Maps](doc/complex-map.md)
  - [Lists](doc/complex-list.md)

You can track the [amount of features we have implemented so far](doc/features.md).

## Related Projects

- [Apache Parquet viewer for Windows 10](https://github.com/aloneguid/parquet-viewer-uwp).
- [Azure Data Lake Analytics Integration](https://github.com/elastacloud/datalake-extractor-parquet).

Download Parquet Viewer from Windows 10 store:

<a href="https://www.microsoft.com/store/apps/9pgb0m8z4j2t?ocid=badge"><img src="https://assets.windowsphone.com/f2f77ec7-9ba9-4850-9ebe-77e366d08adc/English_Get_it_Win_10_InvariantCulture_Default.png" alt="Get it on Windows 10" width="200" /></a>

## Getting started

**Parquet.Net** is redistributed as a [NuGet package](https://www.nuget.org/packages/Parquet.Net). All the code is managed and doesn't have any native dependencies, therefore you are ready to go after referencing the package. This also means the library works on **Windows**, **Linux** and **MacOS X**.

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
	new DataField<int>("id"),
	new DataField<string>("city")
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

Parq is a .NET runtime (written for Windows but will run elsewhere) that brings tooling for inspecting Parquet files to developers. 

It is a command line utility held in a different repository, for which you can find out more by [reading this guide](https://github.com/elastacloud/parq).

## License

Parquet.Net is licensed under the [MIT license](https://github.com/elastacloud/parquet-dotnet/blob/master/LICENSE).

## Contributing

We are desparately looking for new contributors to this projects. It's getting a lot of good use in small to large organisations, however parquet format is complicated and we're out of resources to fix all the issues.

For details on how to start see [this guide](.github/CONTRIBUTING.md). If you are a developer who is interested in Parquet development please [read this guide](doc/parquet-getting-started.md)

> If you need to reference the latest preview version (published on every commit) please use the following NuGet Feed: https://ci.appveyor.com/nuget/parquet-dotnet
