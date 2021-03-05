# Comparison to ParquetSharp

This page is merely a feature comparison between [ParquetSharp](https://github.com/G-Research/ParquetSharp) and Parquet.Net (this library). People at ParquetSharp have done great work and I have used their library myself instead of this one a couple of times. In order to better understand what to choose here is an honest comparison.



| Feature                            | Parquet.Net                                                  | ParquetSharp                                                 |
| ---------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 🚀Speed                             | Faster on smaller datasets (up to 1k rows). Will change in future though. | Faster on larger datasets (over 1k rows)                     |
| Can write to arbitrary stream      | Yes                                                          | No, only local disk files.                                   |
| Complex types (structs, maps etc.) | Full support.                                                | No. Supports only primitive types.                           |
| Cross platform                     | Runs anywhere.                                               | Runs on Windows, Linux (some versions) and MacOSX. 64bit only. |
| ARM support                        | Yes                                                          | No                                                           |
| Apple M1 support                   | Yes                                                          | No                                                           |
| Depends on third-party code        | No                                                           | Yes                                                          |
| Open-Source                        | Yes                                                          | Yes                                                          |

I'm aiming to publish honest performance benchmarks in the near future too.