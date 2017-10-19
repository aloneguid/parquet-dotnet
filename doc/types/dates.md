# Working with Dates

Natively Parquet.Net works with dates using the `DateTimeOffset` CLR type. Although you can specify `DateTime` in schema, the type information is lost when writing to a parquet file, therefore when reading the file we don't know whether you've used `DateTime` or `DateTimeOffset`, therefore when reading `DateTimeOffset` is always used.

The reason we use `DateTimeOffset` is that it causes less confusion when working with dates (all dates are counted in UTC timezone) and has much lighter footprint on memory.