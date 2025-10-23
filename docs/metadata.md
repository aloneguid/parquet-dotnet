# Metadata

There are two places in Parquet file you can store custom [metadata](https://parquet.apache.org/docs/file-format/metadata/) - at *file level* and at *column chunk level*.

## File Metadata

To read and write custom file metadata, you can use `CustomMetadata` property on `ParquetFileReader` and `ParquetFileWriter`, i.e.

```C#
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

The only way to access and manipulate custom metadata is through the low-level API. This API lets you read and write metadata records and fields using the Metadata API. Custom metadata is not data, but metadata that describes other data. Therefore, you can switch between different APIs without affecting the performance of your data stream operations.

## Column Chunk Metadata

Column chunk metadata can be read using `ParquetRowGroupReader.GetCustomMetadata(field)` method. This allows to fetch key-value metadata with zero performance overhead as metadata is stored separate from the column data itself:

```C#
var id = new DataField<int>("id");

using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
    using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
    Dictionary<string, string> kv = rgr.GetCustomMetadata(id);
}
```

To write, use `ParquetRowGroupWriter.WriteColumnAsync` method overload accepting key-value dictionary:

```C#
var id = new DataField<int>("id");

using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
    using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
        await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }),
            new Dictionary<string, string> {
                ["key1"] = "value1",
                ["key2"] = "value2"
            });
    }
}
```

