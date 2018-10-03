# Apache Parquet for .Net Platform

![Icon](doc/img/best.png)

> This documenation is for a Release Candiate of Parquet.Net v3 which is a complete redesign of Parquet library and is not compatible with V2. To see documentation for the latest stable version please [follow this link](https://github.com/elastacloud/parquet-dotnet/tree/final-v2).

![](doc/img/logo_ec.png)

Note that [Elastacloud](https://elastacloud.com/Home) provides commercial support for Parquet.Net, therefore if you need any professional advise or speedy development of new features and bugfixes please write to [parquetsupport@elastacloud.com](mailto:parquetsupport@elastacloud.com).

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

- [Getting Started](#getting-started)
- [Reading Data](doc/reading.md) 
- [Writing Data](doc/writing.md)
- [Complex Types](doc/complex-types.md)
- [Utilities for row-based access](doc/rows.md)
- [Fast Automatic Serialisation](doc/serialisation.md)
- [Declaring Schema](doc/schema.md)
  - [Supported Types](doc/types.md)
- **[parq!!!](doc/parq.md)**

You can track the [amount of features we have implemented so far](doc/features.md).

## Related Projects

- [Apache Parquet viewer for Windows 10](https://github.com/aloneguid/parquet-viewer-uwp).
- [Azure Data Lake Analytics Integration](https://github.com/elastacloud/datalake-extractor-parquet).

Download Parquet Viewer from Windows 10 store:

<a href="https://www.microsoft.com/store/apps/9pgb0m8z4j2t?ocid=badge"><img src="https://assets.windowsphone.com/f2f77ec7-9ba9-4850-9ebe-77e366d08adc/English_Get_it_Win_10_InvariantCulture_Default.png" alt="Get it on Windows 10" width="200" /></a>

## Getting started

**Parquet.Net** is redistributed as a [NuGet package](https://www.nuget.org/packages/Parquet.Net). All the code is managed and doesn't have any native dependencies, therefore you are ready to go after referencing the package. This also means the library works on **Windows**, **Linux** and **MacOS X**.

### General

This intro is covering only basic use cases. Parquet format is more complicated when it comes to complex types like structures, lists, maps and arrays, therefore you should [read this page](doc/parquet-getting-started.md) if you are planning to use them.

### Reading files

In order to read a parquet file you need to open a stream first. Due to the fact that Parquet utilises file seeking extensively, the input stream must be *readable and seekable*. **You cannot stream parquet data!** This somewhat limits the amount of streaming you can do, for instance you can't read a parquet file from a network stream as we need to jump around it, therefore you have to download it locally to disk and then open.

For instance, to read a file `c:\test.parquet` you would normally write the following code:

```csharp
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;

// open file stream
using (Stream fileStream = System.IO.File.OpenRead("c:\\test.parquet"))
{
   // open parquet file reader
   using (var parquetReader = new ParquetReader(fileStream))
   {
      // get file schema (available straight after opening parquet reader)
      // however, get only data fields as only they contain data values
      DataField[] dataFields = parquetReader.Schema.GetDataFields();

      // enumerate through row groups in this file
      for(int i = 0; i < parquetReader.RowGroupCount; i++)
      {
         // create row group reader
         using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
         {
            // read all columns inside each row group (you have an option to read only
            // required columns if you need to.
            DataColumn[] columns = dataFields.Select(groupReader.ReadColumn).ToArray();

            // get first column, for instance
            DataColumn firstColumn = columns[0];

            // .Data member contains a typed array of column data you can cast to the type of the column
            Array data = firstColumn.Data;
            int[] ids = (int[])data;
         }
      }
   }
}
```

### Writing files

Parquet.Net operates on streams, therefore you need to create it first. The following example shows how to create a file on disk with two columns - `id` and `city`.

```csharp
//create data columns with schema metadata and the data you need
var idColumn = new DataColumn(
   new DataField<int>("id"),
   new int[] { 1, 2 });

var cityColumn = new DataColumn(
   new DataField<string>("city"),
   new string[] { "London", "Derby" });

// create file schema
var schema = new Schema(idColumn.Field, cityColumn.Field);

using (Stream fileStream = System.IO.File.OpenWrite("c:\\test.parquet"))
{
   using (var parquetWriter = new ParquetWriter(schema, fileStream))
   {
      // create a new row group in the file
      using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup(2))
      {
         groupWriter.WriteColumn(idColumn);
         groupWriter.WriteColumn(cityColumn);
      }
   }
}
```

## License

Parquet.Net is licensed under the [MIT license](https://github.com/elastacloud/parquet-dotnet/blob/master/LICENSE).

## Contributing

We are desparately looking for new contributors to this projects. It's getting a lot of good use in small to large organisations, however parquet format is complicated and we're out of resources to fix all the issues.

For details on how to start see [this guide](.github/CONTRIBUTING.md). If you are a developer who is interested in Parquet development please [read this guide](doc/parquet-getting-started.md)
