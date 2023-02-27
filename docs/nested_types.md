# Nested Types

Please read [getting started with Parquet](parquet-getting-started.md) to better understand Parquet internals.

## Structs

Structures are the easiest to understand. A structure is simply a container with extra fields i.e. table inside a table cell. From parquet's point of view, there is no difference between a struct's column and top-level column, they are absolutely identical.

Structures are mostly used to logically separate entities and simplify naming for a user. To demonstrate,

The following table:

| name     | address.line1 | address.postcode |
| -------- | ------------- | ---------------- |
| (column) | (column)      | (column)         |

Is identical to

| name   | address                                     |
| ------ | ------------------------------------------- |
| Column | **line1** (column) \| **postcode** (column) |

Each table still has 3 physical columns, they are just named differently.

To make schema for this, we'll use `StructField` which accepts other fields as children:

```csharp
var schema = new ParquetSchema(
   new DataField<string>("name"),
   new StructField("address",
      new DataField<string>("line1"),
      new DataField<string>("postcode")
   ));
```

To write data, we use plain columns:

```csharp
using var ms = new MemoryStream();
using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
    ParquetRowGroupWriter rgw = writer.CreateRowGroup();

    await rgw.WriteColumnAsync(
        new DataColumn(new DataField<string>("name"), new[] { "Joe" }));

    await rgw.WriteColumnAsync(
        new DataColumn(new DataField<string>("line1"), new[] { "Amazonland" }));

    await rgw.WriteColumnAsync(
        new DataColumn(new DataField<string>("postcode"), new[] { "AAABBB" }));
}

```

To read back, again, the data is in plain columns:

```csharp
 ms.Position = 0;

using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
    using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);

    DataField[] dataFields = reader.Schema.GetDataFields();

    DataColumn name = await rg.ReadColumnAsync(dataFields[0]);
    DataColumn line1 = await rg.ReadColumnAsync(dataFields[1]);
    DataColumn postcode = await rg.ReadColumnAsync(dataFields[2]);

    Assert.Equal(new[] { "Joe" }, name.Data);
    Assert.Equal(new[] { "Amazonland" }, line1.Data);
    Assert.Equal(new[] { "AAABBB" }, postcode.Data);
}
```

Note that the only indication that this is a part of struct is `Path` property in the read schema containing struct name:

![](img/struct-path.png)

## Lists and Arrays

Arrays *aka repeatable fields* is a basis for understanding how more complex data structures work in Parquet.

`DataColumn` in Parquet can contain not just a single but multiple values. Sometimes they are called repeated fields (because the data type value repeats) or arrays. In order to create a schema for a repeatable field, let's say of type `int` you could use one of two forms:

```csharp
var field = new DataField<IEnumerable<int>>("items");
```
To check if the field is repeated you can always test `.IsArray` Boolean flag.

Parquet columns are flat, so in order to store an array in the array which can only keep simple elements and not other arrays, you would *flatten* them. For instance to store two elements:

- `[1, 2, 3]`
- `[4, 5]`

in a flat array, it will look like `[1, 2, 3, 4, 5]`. And that's exactly how parquet stores them. Now, the problem starts when you want to read the values back. Is this `[1, 2]` and `[3, 4, 5]` or `[1]` and `[2, 3, 4, 5]`? There's no way to know without an extra information. Therefore, parquet also stores that extra information an an extra column per data column, which is called *repetition levels*. In the previous example, our array of arrays will expand into the following two columns:

| #    | Data Column | Repetition Levels Column |
| ---- | ----------- | ------------------------ |
| 0    | 1           | 0                        |
| 1    | 2           | 1                        |
| 2    | 3           | 1                        |
| 3    | 4           | 0                        |
| 4    | 5           | 1                        |

In other words - it is the level at which we have to create a new list for the current value. In other words, the repetition level can be seen as a marker of when to start a new list and at which level.

To represent this in C# code:

```csharp
var field = new DataField<IEnumerable<int>>("items");
var column = new DataColumn(
   field,
   new int[] { 1, 2, 3, 4, 5 },
   new int[] { 0, 1, 1, 0, 1 });
```

