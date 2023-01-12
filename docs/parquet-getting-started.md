# Getting started with Parquet development

If you are a complete novice to Parquet we would recommend starting with these documents:

- [The striping and assembly algorithms from the Dremel paper](https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) (what Parquet is based on)
- To better understand Parquet, especially what repetition and definition levels are - [Dremel made simple with Parquet](https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html)

## Encodings and types

If you are looking for a description of parquet encodings please [follow this link](https://github.com/apache/parquet-format/blob/master/Encodings.md).

To understand how Parquet represents rich logical types [read this](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)

## Reference implementations

There are already working implementations in other languages we find useful to check we are doing things right or when stuck understanding how a particular feature is supposed to work.

[parquet-mr](https://github.com/apache/parquet-mr) is an official specification repository containing Thrift definitions for data structures within the Parquet file. This spec is referenced by any library that implements Parquet.

[parquet-mr](https://github.com/apache/parquet-mr) is an official Java implementation, somewhat over-engineered, however the most stable.

[fastparquet](https://github.com/dask/fastparquet) is probably the best implementation for Python, and it is extremely easy to follow. This is also our library of choice to work with the parquet format (of course, before parquet-dotnet was created :) )

[parquet-cpp](https://github.com/apache/parquet-cpp) is an *awful* implementation using the C++ language, struggling both with code quality and compatibility.  I would not recommend looking at it if you are new to parquet.

## 3rd Party Libraries

[Snappy Sharp](https://github.com/jeffesp/Snappy.Sharp) was used to compress and decompress via Snappy Algorithm. It is now replaced by my own implementation [IronCompress](https://github.com/aloneguid/IronCompress.

## Alternatives

[ParquetSharp](https://github.com/G-Research/ParquetSharp) is another .NET library for working with parquet files, which is a great work on wrapping C++ implementation with .NET API. Despite being a native wrapper, it's somewhat slower than this managed library starting from v4.2.

