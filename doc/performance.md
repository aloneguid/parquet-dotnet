# Performance Tuning

> Work on performance is postponed until v1.7

## Performance Comparison

All tests are performed using v1.5 alpha, in the same initial conditions on Intel i7 CPU with 16GB RAM.

It's worth noting that parquet-dotnet has never been optimised for performance yet.

### Case 1

**File:** customer.impala.parquet

**Size:** 25Mb

**Rows**: 150'000

||Read|Write (uncompressed)|Write (gzip)|Write (snappy)|
|-|-|-|-|-|
|fastparquet|273 ms|266 ms|5'633 ms|307 ms|
|parquet-dotnet|354 ms|351 ms|1'317 ms|351 ms|
|Scala (Spark)|-|-|-|-|


![Perf00](img/perf00.png)

In this test Apache Spark cannot read the file due to an internal bug.

As you can see, Parquet.Net loses to fastparquet slightly in everything except for writing gzip compressed parquet files.
