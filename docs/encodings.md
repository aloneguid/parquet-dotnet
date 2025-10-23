# Configuring encodings

## Strings

By default, Parquet.Net is using [dictionary encoding](https://parquet.apache.org/docs/file-format/data-pages/encodings/#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8) for string columns, when at most `80 %` of values are unique.

You can change this threshold by modifying `ParquetOptions.DictionaryEncodingThreshold`, or even turn off dictionary encoding by settings `UseDictionaryEncoding` to `false`.

## Numbers

By default, Parquet.Net will use [delta encoding](https://parquet.apache.org/docs/file-format/data-pages/encodings/#a-namedeltaencadelta-encoding-delta_binary_packed--5) for `int` and `long` columns, which is more efficient than plain encoding.

If you need to turn off delta encoding, set `ParquetOptions.UseDeltaBinaryPackedEncoding` to `false`.