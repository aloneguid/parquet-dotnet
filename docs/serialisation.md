# Class Serialisation

Parquet library is generally extremely flexible in terms of supporting internals of the Apache Parquet format and allows you to do whatever the low level API allow to. However, in many cases writing boilerplate code is not suitable if you are working with business objects and just want to serialise them into a parquet file. 

For this reason the library implements a fast serialiser/deserialiser for parquet files.

They both generate IL code in runtime for a class type to work with, therefore there is a tiny bit of a startup compilation delay which in most cases is negligible. Once the class is serialised at least once, further operations become blazingly fast. Note that this is still faster than reflection. In general, we see around *x40* speed improvement comparing to reflection on relatively large amounts of data (~5 million records).

## Customisation

Customisation very closely mimics `System.Text.Json` in a sense that attribute names, their amount and meaning are almost identical. For instance, to rename property when serialising to JSON you would use `JsonPropertyNameAttribute` in `System.Text.Json` and `ParquetPropertyNameAttribute` in this library and so on. This decision is deliberate to minimise any learning curve when working with this library.

