# Getting started with Parquet development

If you are a complete novice to Paruqet we would recommend starting with these documents:

- [The striping and assembly algorithms from the Dremel paper](https://github.com/Parquet/parquet-mr/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) (what Parquet is based on)
- To better understand Parquet, especially what repetition and definition levels are - [Dremel made simple with Parquet](https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html)

## Encodings and types

If you are looking for a description of parquet encodings please [follow this link](https://github.com/Parquet/parquet-format/blob/master/Encodings.md).

To understand how Parquet represents rich logical types [read this](https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md)

## Reference implementations

There are already working implementations in other languages we find useful to check we are doing things right or when stuck understanding how particular feature is supposed to work.

[parquet-mr](https://github.com/Parquet/parquet-mr) is an official specification repository containing Thrift definitions for data structures within the Parquet file. This spec is referenced by any library that impelments Parquet.

[fastparquet](https://github.com/dask/fastparquet) is probably the best implementation for Python, and it's extremely easy to follow. This is also our library of choice to work with parquet format (of course, before parquet-dotnet was created :) )

[parquet-mr](https://github.com/Parquet/parquet-mr) is an official Java implementation, somewheat overengineered, however the most stable.

[parquet-cpp](https://github.com/Parquet/parquet-cpp) is an *awful* implementation in C++ language, struggling both with code quality and compatibility and I wouldn't recommend looking at it if you're new to parquet.


