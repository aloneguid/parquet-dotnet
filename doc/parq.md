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

By default **parq** displays the first 10 rows of the source file, however you can override it with `--max-rows` option.

## Converting to CSV

```powershell
parq convert <path-to-file> -f csv
```

converts to CSV, for instance:

```
c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment
1,Customer#000000001,IVhzIApeRb ot,c,E,15,25-989-741-2988,711.56,BUILDING,to the even, regular platelets. regular, ironic epitaphs nag e
2,Customer#000000002,XSTf4,NCwDVaWNe6tEgvwfmRchLXak,13,23-768-687-3665,121.65,AUTOMOBILE,l accounts. blithely ironic theodolites integrate boldly: caref
3,Customer#000000003,MG9kdTD2WBHm,1,11-719-748-3364,7498.12,AUTOMOBILE, deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov
4,Customer#000000004,XxVSJsLAGtn,4,14-128-190-5944,2866.83,MACHINERY, requests. final, regular ideas sleep final accou  5,Customer#000000005,KvpyuHCplrB84WgAiGV6sYpZq7Tj,3,13-750-942-6364,794.47,HOUSEHOLD,n accounts will have to unwind. foxes cajole accor
```

Note that CSV conversion is not intended to be used with complex data types and is best to be used with flat tables only.

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