# parquet-dotnet [![Visual Studio Team services](https://img.shields.io/vso/build/elastacloud/bad95de3-58b5-428f-96a5-1b28793d0a5f/13.svg?style=plastic)]()

A .NET library to read and write [Apache Parquet](https://github.com/Parquet) files.

## Why

Parquet library is mostly available for Java, C++ and Python, which somewhat limits .NET/C# platform in big data applications. Whereas C# is a great language we still don't have anything good in this area.

This project is aimed to fix this problem.

## Roadmap

We have just started to work on this library, contributors are welcome.

|Phase|Description|State|
|-----|-----------|-----|
|1|Implement reader which can understand the first test file (alltypes_plain.parquet). This is using a variety of encodings an no compression. Inline columns are not supported. Understand how to return results with minimum of boxing/unboxing. Support NULL values.|finishing|
|2|Implement readers for any types not mentioned in phase 1. Implement writer for all types that reader supports. Publish alpha version on NuGet.|planning|
|3|Implement writer which can do the same types as in phase 1 and 2.|planning|
|4|Support GZIP decompression/compression|planning|
|5|Support nested columns|planning|

## Tools

### parq

This tools gives a simple schema inspector which lists out the columns found in a Parquet data set. 

To use, run ```dotnet parq.dll InputFilePath=path/to/file.parquet```

![Parq](docs/img/parq.jpg)

## License

.NET Core (including the corefx repo) is licensed under the [MIT license](https://github.com/elastacloud/parquet-dotnet/blob/master/LICENSE).


## Notes

We use [Thrift for .NET Core](https://github.com/apache/thrift/tree/master/lib/netcore) project to read Thrift specification which is take from the official [parquet-format](https://github.com/Parquet/parquet-format) repository as is. This depends on .NET Standard 1.6 which makes it a requirement for this project as well.

To regenerate thrift classes use `scripts\thriftgen.ps1`. Project is set to define `SILVERLIGHT` constant as .NET Standard is not fully compatible with this generator, but tested to be working in silverlight compatibility mode.

Parquet compatibility samples: https://github.com/Parquet/parquet-compatibility

## Useful links

- [List of Parquet encodings](https://github.com/Parquet/parquet-format/blob/master/Encodings.md)
- [Parquet Logical Types](https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md)
- [The striping and assembly algorithms from the Dremel paper](https://github.com/Parquet/parquet-mr/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) (what Parquet is based on)
- To better understand Parquet, especially what repetition and definition levels are - [Dremel made simple with Parquet](https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html)