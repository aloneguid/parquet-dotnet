# Apache Parquet for .NET 

[![NuGet](https://img.shields.io/nuget/v/Parquet.Net.svg)](https://www.nuget.org/packages/Parquet.Net)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Parquet.Net)](https://www.nuget.org/packages/Parquet.Net)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/aloneguid/parquet-dotnet)
![GitHub repo size](https://img.shields.io/github/repo-size/aloneguid/parquet-dotnet) ![](https://img.shields.io/badge/AI-NO%20LLMs-blue)
![GitHub forks](https://img.shields.io/github/forks/aloneguid/parquet-dotnet)

👉 [home + documentation](https://www.aloneguid.uk/projects/parquet-dotnet/)  👈

[![Icon](https://www.aloneguid.uk/projects/parquet-dotnet/banner.png)](https://www.aloneguid.uk/projects/parquet-dotnet/)


**Fully managed, safe, extremely fast** .NET library to 📖read and ✍️write [Apache Parquet](https://parquet.apache.org/) files designed for .NET world (not a wrapper). Targets only modern .NET runtimes such as `.NET 10` and `.NET 8`.

> [!IMPORTANT]
> This project does **not use LLMs** for writing code. It **will not accept LLMs** for issues. No pull requests written by LLMs will be accepted. No LLMs for comments anywhere. Attempts to circumvent this may prevent your future contributions to this repository.

# Features at a glance

- 0️⃣ **Has zero dependencies** - pure library that just works anywhere .NET works i.e. desktops, servers, phones, watches and so on.
- 🚀**Really fast.** Faster than Python and Java, and alternative C# implementations out there. It's even faster than native C++ implementations. **Hardware acceleration** is added in more and more places.
- 🏠**NET native.** Designed to utilise .NET and made for .NET developers, not the other way around.
- ❤️‍🩹**Not a "wrapper"** that forces you to fit in. It's the other way around — forces Parquet to fit into .NET.
- 🦄**Unique Features**:
  - The only library that supports dynamic schemas.
  - Supports all parquet types, encodings and compressions.
  - Fully supports [C# class serialization](#high-level-api), for all simple and **complex** Parquet types.
  - Provides **low-level**, high-level, and untyped API.
  - Access to file and column metadata.
  - Fine-tune encodings per column.
  - [Integration with DataFrames](#dataframe-support) (`Microsoft.Data.Analysis`).


## Used by

- [Azure Cosmos DB Desktop Data Migration Tool](https://github.com/AzureCosmosDB/data-migration-desktop-tool)
- [RavenDB](https://github.com/ravendb/ravendb) - An ACID NoSQL Document Database
- [Cinchoo ETL](https://github.com/Cinchoo/ChoETL) - An ETL framework for .NET
- [ParquetViewer](https://github.com/mukunku/ParquetViewer) - Simple Windows desktop application for viewing & querying Apache Parquet files
- [ML.NET](https://github.com/dotnet/machinelearning) - Machine Learning for .NET
- [PSParquet](https://github.com/Agazoth/PSParquet) - PowerShell Module for Parquet
- [Omni Loader](https://www.omniloader.com) - Self-tuning Database Migration Accelerator
- [Contoso Data Generator V2](https://github.com/sql-bi/Contoso-Data-Generator-V2) - Sample data generator
- [Recfuence]() - An analysis of YouTube's political influence through recommendations
- [Kusto-loco](https://github.com/NeilMacMullen/kusto-loco) - C# KQL query engine with flexible I/O layers and visualization
- [DeltaIO](https://github.com/aloneguid/delta) - Delta Lake implementation in pure .NET
- [Personal Data Warehouse](https://github.com/BlazorData-Net/PersonalDataWarehouse) - Import(Excel/Parquet/SQL/Fabric)-Transform(C#/Python)-Report(SSRS)
- [FastBCP](https://fastbcp.arpe.io/) - Export to parquet files in parallel from Oracle, SQL Server, MySQL, PostgreSQL, ODBC, Teradata, Netezza, SAP HANA, ClickHouse in one command line (Windows & Linux)
- [Parquet.FSharp](https://github.com/rob-earwaker/parquet-fsharp) - Adds first-class support for F# types such as records, options, lists and discriminated unions

*...raise a PR to appear here...*