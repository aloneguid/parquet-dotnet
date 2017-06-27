# parquet-dotnet [![Build status](https://ci.appveyor.com/api/projects/status/w3o50mweytm85uxb?svg=true)](https://ci.appveyor.com/project/aloneguid/parquet-dotnet)

![Icon](doc/img/icon.png)

A .NET library to read and write [Apache Parquet](https://github.com/Parquet) files. Supports .NET 4.5.1 and .NET Standard 1.6.

## Why

Parquet library is mostly available for Java, C++ and Python, which somewhat limits .NET/C# platform in big data applications. Whereas C# is a great language we still don't have anything good in this area.

This project is aimed to fix this problem.

## Roadmap

We have just started to work on this library, contributors are welcome.

|Phase|Description|State|
|-----|-----------|-----|
|1|Implement reader which can understand the first test file (alltypes_plain.parquet). This is using a variety of encodings an no compression. Inline columns are not supported. Understand how to return results with minimum of boxing/unboxing. Support NULL values.|complete|
|2|Implement readers for any types not mentioned in phase 1. Implement writer for all types that reader supports. Publish alpha version on NuGet.|in progress|
|3|Support GZIP and SNAPPY decompression/compression|planning|
|4|Integrate with popular products like Azure Data Lakes|planning|

## Tools

### parq

This tools gives a simple schema inspector which lists out the columns found in a Parquet data set. 

To use, run ```dotnet parq.dll InputFilePath=path/to/file.parquet```

![Parq](doc/img/parq.JPG)

## License

parquet-dotnet is licensed under the [MIT license](https://github.com/elastacloud/parquet-dotnet/blob/master/LICENSE).

## Contributing

All contributions are welcome. For details on how to start see [this guide](CONTRIBUTING.md)
