# Writing Data

You can write data by constructing an instance of [ParquetWriter class](../src/Parquet/ParquetWriter.cs) with one of it's factory methods.

Writing files is a multi stage process, giving you the full flexibility on what exactly to write to it:

1. Create `ParquetWriter` passing it a *file schema* and a *writeable stream*. You should have declared file schema beforehand.
2. Create a row group writer by calling to `writer.CreateRowGroup()`.
3. Keep calling `.WriteAsync()` by passing the data columns with data you want to write. Note that the order of data columns you are writing must match the order of data fields declared in the schema.
4. When required, repeat from step (2) to create more row groups. A row groups is like a physical data partition that should fit in memory for processing. It's a guess game how much data should be in a single row group, but a number of at least 5 thousand rows per column is great. Remember that parquet format works best on large chunks of data.

```csharp
// create file schema
var schema = new ParquetSchema(
    new DataField<int>("id"),
    new DataField<string>("city"));

//create data columns with schema metadata and the data you need
var idColumn = new DataColumn(
   schema.DataFields[0],
   new int[] { 1, 2 });

var cityColumn = new DataColumn(
   schema.DataFields[1],
   new string[] { "London", "Derby" });

using(Stream fileStream = System.IO.File.OpenWrite("c:\\test.parquet")) {
    using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream)) {
        parquetWriter.CompressionMethod = CompressionMethod.Gzip;
        parquetWriter.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;
        // create a new row group in the file
        using(ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup()) {
            await groupWriter.WriteColumnAsync(idColumn);
            await groupWriter.WriteColumnAsync(cityColumn);
        }
    }
}
```

To read more about DataColumn, see [this page](column.md).

### Specifying Compression Method and Level

After constructing `ParquetWriter` you can optionally set compression method ([`CompressionMethod`](../src/Parquet/CompressionMethod.cs)) and/or compression level ([`CompressionLevel`](https://learn.microsoft.com/en-us/dotnet/api/system.io.compression.compressionlevel?view=net-7.0)) which defaults to `Snappy`.  Unless you have specific needs to override compression, the default are very reasonable.

For instance, to set compression to gzip/optimal:

```csharp
using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream)) {
    parquetWriter.CompressionMethod = CompressionMethod.Gzip;
    parquetWriter.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;
    // create row groups and write...
}
```

### Appending to Files

This lib supports pseudo appending to files, however it's worth keeping in mind that *row groups are immutable* by design, therefore the only way to append is to create a new row group at the end of the file. It's worth mentioning that small row groups make data compression and reading extremely ineffective, therefore the larger your row group the better.

This should make you The following code snippet illustrates this:

```csharp
//write a file with a single row group
var schema = new ParquetSchema(new DataField<int>("id"));
var ms = new MemoryStream();

using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
    using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
        await rg.WriteColumnAsync(new DataColumn(schema.DataFields[0], new int[] { 1, 2 }));
    }
}

//append to this file. Note that you cannot append to existing row group, therefore create a new one
ms.Position = 0;    // this is to rewind our memory stream, no need to do it in real code.
using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, append: true)) {
    using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
        await rg.WriteColumnAsync(new DataColumn(schema.DataFields[0], new int[] { 3, 4 }));
    }
}

//check that this file now contains two row groups and all the data is valid
ms.Position = 0;
using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
    Assert.Equal(2, reader.RowGroupCount);

    using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
        Assert.Equal(2, rg.RowCount);
        Assert.Equal(new int[] { 1, 2 }, (await rg.ReadColumnAsync(schema.DataFields[0])).Data);
    }

    using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
        Assert.Equal(2, rg.RowCount);
        Assert.Equal(new int[] { 3, 4 }, (await rg.ReadColumnAsync(schema.DataFields[0])).Data);
    }

}
```

Note that you have to specify that you are opening `ParquetWriter` in **append** mode in it's constructor explicitly - `new ParquetWriter(new Schema(id), ms, append: true)`. Doing so makes parquet.net open the file, find the file footer and delete it, rewinding current stream position to the end of actual data. Then, creating more row groups simply writes data to the file as usual, and `.Dispose()` on `ParquetWriter` generates a new file footer, writes it to the file and closes down the stream.

Please keep in mind that row groups are designed to hold a large amount of data (5'0000 rows on average) therefore try to find a large enough batch to append to the file. Do not treat parquet file as a row stream by creating a row group and placing 1-2 rows in it, because this will both increase file size massively and cause a huge performance degradation for a client reading such a file.

### Custom Metadata

To read and write custom file metadata, you can use `CustomMetadata` property on `ParquetFileReader` and `ParquetFileWriter`, i.e.

```csharp
var ms = new MemoryStream();
var schema = new ParquetSchema(new DataField<int>("id"));

//write
using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
    writer.CustomMetadata = new Dictionary<string, string> {
        ["key1"] = "value1",
        ["key2"] = "value2"
    };

    using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
        await rg.WriteColumnAsync(new DataColumn(schema.DataFields[0], new[] { 1, 2, 3, 4 }));
    }
}

//read back
using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
    Assert.Equal("value1", reader.CustomMetadata["key1"]);
    Assert.Equal("value2", reader.CustomMetadata["key2"]);
}
```

### Complex Types

To write complex types (arrays, lists, maps, structs) read [this guide](nested_types.md).
