# Apache Parquet for .Net Platform

![Icon](doc/img/banner.svg)

## Status

[![NuGet](https://img.shields.io/nuget/v/Parquet.Net.svg)](https://www.nuget.org/packages/Parquet.Net) ![Nuget](https://img.shields.io/nuget/dt/Parquet.Net)

**Fully portable, managed** .NET library to read and write [Apache Parquet](https://parquet.apache.org/) files. Supports:

- `.NET 5.0`, `.NET Core 3.1 (LTS)`, `.NET Core 2.1 (LTS)`.
- `.NET Standard 1.4` and up (for those who are in a tank that means it supports `.NET Core` (all versions) implicitly)
- `.NET 4.6.1` and up via `.NET Standard 1.4`. 

Runs on all flavours of Windows, Linux, MacOS, mobile devices (iOS, Android) via [Xamarin](https://www.xamarin.com/), [gaming consoles](doc/xboxone.md) or anywhere .NET Standard runs which is a lot!

Support Web Assembly is coming (email me if you are interested in details).

> Performs integration tests with **parquet-mr** (original Java parquet implementation) to test for identical behaviour. I am planning to add more third-party platforms integration as well.

## Why

Parquet is a de facto physical storage format in big data applications, including [Apache Spark](https://spark.apache.org/), as well as newly emerging [Delta Lake](https://delta.io/) and lakehouse architectures. It's really easy to read and write data if you are using one of those platforms, however outside of them using Parquet is very hard or impossible. The easiest way to read and write parquet is [using PyArrow](https://arrow.apache.org/docs/python/parquet.html), and good luck with any other approach (Java or C++ versions are unusable in their raw form).

Note that [ParquetSharp](https://github.com/G-Research/ParquetSharp) provides a P/Invoke wrapper around parquet-cpp library with [all the consequences](doc/parquetsharp.md).

## Who

Parquet.Net is used by many small and large organisations for production workloads. If you are one of them, please email [ivan.gavryliuk@outlook.com](mailto:ivan.gavryliuk@outlook.com) to be displayed here.

## Performance

How do we compare to other parquet implementations? We are fast and getting faster with every release. Parquet.Net is dedicated to low memory footprint, small GC pressure and low CPU usage. In this test we are using a file with **8 columns** and **150'000** rows, and the result is:

![Perf00](doc/img/perf00.png)

| |Parquet.Net (.NET Core 2.1)|Fastparquet (python)|parquet-mr (Java)|
|-|---------------------------|--------------------|-----------------|
|Read|14ms|22ms|151ms|
|Write (uncompressed)|4ms|26ms|617ms|
|Write (gzip)|11ms|200ms|1'974ms|

All the parties in this test were given 10 iteration and time was taken as an average. *Parquet-Mr* was even given a warm-up time being the slowest candidate, so it can fit on the chart.

## Index

- [Getting Started](#getting-started)
- [Reading Data](doc/reading.md) 
- [Writing Data](doc/writing.md)
- [Complex Types](doc/complex-types.md)
- [Row-Based API](doc/rows.md)
- [Fast Automatic Serialisation](doc/serialisation.md)
- [Declaring Schema](doc/schema.md)
  - [Supported Types](doc/types.md)
- [Sponsorship](#sponsorship)

You can track the [amount of features we have implemented so far](doc/features.md).

## Getting started

**Parquet.Net** is redistributed as a [NuGet package](https://www.nuget.org/packages/Parquet.Net). All the code is managed and doesn't have any native dependencies, therefore you are ready to go after referencing the package. This also means the library works on **Windows**, **Linux** and **MacOS X** (including M1).

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

using (Stream fileStream = System.IO.File.Create("c:\\test.parquet"))
{
   using (var parquetWriter = new ParquetWriter(schema, fileStream))
   {
      // create a new row group in the file
      using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
      {
         groupWriter.WriteColumn(idColumn);
         groupWriter.WriteColumn(cityColumn);
      }
   }
}
```

### Row-Based Access

Parquet.Net includes [API for row-based access](doc/rows.md) that simplify parquet programming at the expense of memory, speed and flexibility. We recommend using column based approach when you can (examples above) however if not possible use these API as we constantly optimise for speed and use them internally ourselves in certain situations.

## Who Uses Parquet.Net?

- [ML.NET](https://github.com/dotnet/machinelearning).
- Some parts of the popular [RavenDB NoSQL](https://ravendb.net/) database engine.
- Native Windows [Parquet Viewer app](https://github.com/mukunku/ParquetViewer).
- [Recfluence](https://github.com/markledwich2/Recfluence) YouTube analytics.

and [many more](https://github.com/aloneguid/parquet-dotnet/network/dependents). Want to be listed here? Just raise a PR.

## Contributing

We are desperately looking for new contributors to this project. It's getting a lot of good use in small to large organisations, however parquet format is complicated and we're out of resources to fix all the issues.

For details on how to start see [this guide](.github/CONTRIBUTING.md). If you are a developer who is interested in Parquet development please [read this guide](doc/parquet-getting-started.md)

## Sponsorship

This framework is free and can be used for free, open source and commercial applications. Parquet.Net (all code, NuGets and binaries) are under the [MIT License (MIT)](https://github.com/aloneguid/parquet-dotnet/blob/master/LICENSE). It's battle-tested and used by many awesome people and organisations. So hit the magic ⭐️ button, we appreciate it!!! 🙏 Thx!

The core team members, Parquet.Net contributors and contributors in the ecosystem do this open source work in their free time. If you use Parquet.Net, and you'd like us to invest more time on it, please donate by pressing the ❤ **Sponsor** button on top of this page. This project increases your income/productivity/usability too.

If your company/project is using Parquet.Net we'd be happy to list your logo here on the front page with your kind permission, absolutely for free. Please contact [ivan.gavryliuk@outlook.com](mailto:ivan.gavryliuk@outlook.com) with details and graphics attached.
