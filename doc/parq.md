# PARQ (Global Tool)

Since v3.1 parquet repository includes an amazing [.NET Core Global Tool](https://docs.microsoft.com/en-us/dotnet/core/tools/global-tools) called **parq** which serves as a first class command-line client to perform various funtions on parquet files.

## Installing

Installing is super easy with *global tools*, just go to the terminal and type `dotnet tool install -g parq` and it's done. Note that you need to have at least **.NET Core 2.1 SDK** isntalled on your machine, which you probably have as a hero .NET developer.

## Commands

### Viewing Schema

To view schema type

```powershell
parq schema <path-to-file>
````

which produces an output similar to:

![Parq Schema](img/parq-schema.png)

### Converting to JSON

```powershell
parq convert <path-to-file>
```

converts to multiline json, for instance:


```powershell
parq convert all_var1.parquet

{"addresses": [{"line1": "Dante Road", "name": "Head Office", "openingHours": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18], "postcode": "SE11"}, {"line1": "Somewhere Else", "name": "Small Office", "openingHours": [6, 7, 19, 20, 21, 22, 23], "postcode": "TN19"}], "cities": ["London", "Derby"], "comment": "this file contains all the permunations for nested structures and arrays to test Parquet parser", "id": 1, "location": {"latitude": 51.2, "longitude": 66.3}, "price": {"lunch": {"max": 2, "min": 1}}}
{"addresses": [{"line1": "Dante Road", "name": "Head Office", "openingHours": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18], "postcode": "SE11"}, {"line1": "Somewhere Else", "name": "Small Office", "openingHours": [6, 7, 19, 20, 21, 22, 23], "postcode": "TN19"}], "cities": ["London", "Derby"], "comment": "this file contains all the permunations for nested structures and arrays to test Parquet parser", "id": 1, "location": {"latitude": 51.2, "longitude": 66.3}, "price": {"lunch": {"max": 2, "min": 1}}}
```

you can optionally pretty-print the JSON output by specifying a `-p` parameter:

```powershell
parq convert struct_plain.parquet -p

[
{
  "isbn": "12345-6",
  "author": {
    "firstName": "Ivan",
    "lastName": "Gavryliuk"
  }
},
{
  "isbn": "12345-7",
  "author": {
    "firstName": "Richard",
    "lastName": "Conway"
  }
}]
```

As JSON is usually human readable you can use this command to view the file.

By default **parq** displays the first 10 rows of the source file, however you can override it with `--max-rows` option.

### Viewing Internal Metadata

Internal metadata is grabbed from parquet file internals and describes pretty much everything we know about the file. This metadata is not by default exposed from Parquet.Net API as it's hard to work with from the user perspective, however it can be extremely useful for performance tuning and general understanding how a particular file is structured.

To view this metadata, type

```powershell
parq meta <path-to-file>
````

sample output:

```bash
parq meta stats_test.parquet
```

```
parq v1.0.0

File Metadata
Created By parquet-mr version 1.8.3 (build aef7230e114214b7cc962a8f3fc5aeed6ce80828)
Total Rows 2
Version    1

Key-Value Metadata
org.apache.spark.sql.parquet.row.metadata {"type":"struct","fields":[{"name":"isbn","type":"string","nullable":true,"metadata":{}},{"name":"author","type":"string","nullable":true,"metadata":{}}]}

Row Groups

  Row Group #0
  Total Rows      2
  Total Byte Size 162 (0.16 KiB)

    Column #0
    File Offset            4
    File Path
    Codec                  UNCOMPRESSED
    Data Page Offset       4
    Dictionary Page Offset 0
    Index Page Offset      0
    Encodings              RLE, PLAIN, BIT_PACKED
    Total Values           2
    Path in Schema         isbn
    Compressed Size        67 (0.07 KiB)
    Uncompressed Size      67 (0.07 KiB)
    Type                   BYTE_ARRAY
    Statistics
      Null Count     0
      Distinct Count undefined
      Min            12345-6
      Max            12345-7

    Column #1
    File Offset            71
    File Path
    Codec                  UNCOMPRESSED
    Data Page Offset       71
    Dictionary Page Offset 0
    Index Page Offset      0
    Encodings              RLE, PLAIN, BIT_PACKED
    Total Values           2
    Path in Schema         author
    Compressed Size        95 (0.09 KiB)
    Uncompressed Size      95 (0.09 KiB)
    Type                   BYTE_ARRAY
    Statistics
      Null Count     0
      Distinct Count undefined
      Min            Ivan Gavryliuk
      Max            Richard Conway

```

### More Commands

They are coming soon, please leave your comments in the issue tracker in terms of what you would like to see next.