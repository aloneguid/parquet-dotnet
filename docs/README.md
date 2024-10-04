# Apache Parquet for .NET 
[![NuGet](https://img.shields.io/nuget/v/Parquet.Net.svg)](https://www.nuget.org/packages/Parquet.Net) [![NuGet Version](https://img.shields.io/nuget/vpre/Parquet.Net)](https://www.nuget.org/packages/Parquet.Net) [![Nuget](https://img.shields.io/nuget/dt/Parquet.Net)](https://www.nuget.org/packages/Parquet.Net) ![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/aloneguid/parquet-dotnet) ![GitHub repo size](https://img.shields.io/github/repo-size/aloneguid/parquet-dotnet) ![GitHub forks](https://img.shields.io/github/forks/aloneguid/parquet-dotnet)





![Icon](https://github.com/aloneguid/parquet-dotnet/blob/master/docs/img/banner.png?raw=true)

**Fully managed, safe, extremely fast** .NET library to 📖read and ✍️write [Apache Parquet](https://parquet.apache.org/) files designed for .NET world (not a wrapper). Targets `.NET 8`, `.NET 7`, `.NET 6.0`, `.NET Core 3.1`,  `.NET Standard 2.1` and `.NET Standard 2.0`.

Whether you want to build apps for Linux, MacOS, Windows, iOS, Android, Tizen, Xbox, PS4, Raspberry Pi, Samsung TVs or much more, Parquet.Net has you covered.

## Features at a glance

- 0️⃣ **Has zero dependencies** - pure library that just works anywhere .NET works i.e. desktops, servers, phones, watches and so on.
- 🚀**Really fast.** Faster than Python and Java, and alternative C# implementations out there. It's often even faster than native C++ implementations.
- 🏠**NET native.** Designed to utilise .NET and made for .NET developers, not the other way around.
- ❤️‍🩹**Not a "wrapper"** that forces you to fit in. It's the other way around - forces parquet to fit into .NET.
- 🦄**Unique Features**:
  - The only library that supports [dynamic](https://aloneguid.github.io/parquet-dotnet/writing.html) schemas.
  - Supports all parquet [types](https://aloneguid.github.io/parquet-dotnet/nested-types.html), encodings and compressions.
  - Fully supports [C# class serialization](https://aloneguid.github.io/parquet-dotnet/serialisation.html), for all simple and **complex** Parquet types.
  - Provides **low-level**, [high-level](https://aloneguid.github.io/parquet-dotnet/serialisation.html), and [untyped](https://aloneguid.github.io/parquet-dotnet/untyped-serializer.html) API.
  - Access to [file and column metadata](https://aloneguid.github.io/parquet-dotnet/metadata.html).
  - [Integration with DataFrames](https://aloneguid.github.io/parquet-dotnet/dataframe.html) (`Microsoft.Data.Analysis`).


## Links

- [Quick Start](https://aloneguid.github.io/parquet-dotnet/starter-topic.html#quick-start).
- [Full Documentation](https://aloneguid.github.io/parquet-dotnet/starter-topic.html).

## UI

This repository now includes an implementation of parquet desktop viewer application called **Floor** (parquet floor, get it?). It's cross-platform, self-contained executable made with Avalonia, and is compiled for Linux, Windows and MacOS.

![](https://github.com/aloneguid/parquet-dotnet/blob/master/docs/img/floor.gif?raw=true)

**Floor** is not meant to be the best parquet viewer on the planet, but just a reference implementation. There are probably better, more feature-rich applications out there.

### Installing

Download it from the [releases section](https://github.com/aloneguid/parquet-dotnet/releases). On Windows, you can install it with winget - `winget install aloneguid.ParquetDotnet.floor`.

## Used by

- [Azure Cosmos DB Desktop Data Migration Tool](https://github.com/AzureCosmosDB/data-migration-desktop-tool).
- [RavenDB - An ACID NoSQL Document Database](https://github.com/ravendb/ravendb).
- [Cinchoo ETL: An ETL framework for .NET](https://github.com/Cinchoo/ChoETL).
- [ParquetViewer: Simple Windows desktop application for viewing & querying Apache Parquet files](https://github.com/mukunku/ParquetViewer).
- [ML.NET: Machine Learning for .NET](https://github.com/dotnet/machinelearning).
- [PSParquet: PowerShell Module for Parquet](https://github.com/Agazoth/PSParquet).
- [Omni Loader: Self-tuning Database Migration Accelerator](https://www.omniloader.com).
- [Contoso Data Generator V2 : sample data generator](https://github.com/sql-bi/Contoso-Data-Generator-V2).
- [Recfuence - An analysis of YouTube's political influence through recommendations]().
- [Kusto-loco - C# KQL query engine with flexible I/O layers and visualization](https://github.com/NeilMacMullen/kusto-loco).

*...raise a PR to appear here...*

## Contributing

See the [contribution page](https://aloneguid.github.io/parquet-dotnet/contributing.html). The first important thing you can do is **simply star ⭐ this project**.
