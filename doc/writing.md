# Writing Data

You can write data by constructing an instance of [ParquetWriter class](../src/Parquet/ParquetWriter.cs) or using one of the helper classes.

Writing files is a multi stage process, giving you the full flexibility on what exactly to write to it:

1. Create `ParquetWriter` passing it a *file schema* and a *writeable stream*. You should have declared file schema beforehand.
2. Create a row group writer by calling to `writer.CreateRowGroup(rowSize)`.
3. Keep calling `.Write()` by passing the data columns with data you want to write. Note that the order of data columns you are writing must match the order of data fields declared in the schema.
4. When required, repeat from step (2) to create more row groups. It's not recommended to have more than 5'000 rows in a single row group for performance reasons.

```csharp
//create data columns with schema metadata and the data you need
var idColumn = new DataColumn(
   new DataField<int>("id"),
   new int[] { 1, 2 });

var cityColumn = new DataColumn(
   new DataField<string>("city"),
   new string[] { "London", "Derby" });

// create file schema
var schema = new Schema(idColumn.Field, cityColumn.Field);

using (Stream fileStream = System.IO.File.OpenWrite("c:\\test.parquet"))
{
   using (var parquetWriter = new ParquetWriter(schema, fileStream))
   {
      // create a new row group in the file
      using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
      {
         groupWriter.WriteColumn(idColumn);
         groupWriter.WriteColumn(cityColumn);
      }
   }
}
```

## Specifying Compression Method

After constructing `ParquetWriter` you can optionally set compression method (`CompressionMethod` property) which defaults to `Snappy`. The possible values are:

- `None` for no compression. This is the fastest way to write files, however they may end up slightly larger.
- `Snappy` is the default level and is a perfect balance between compression and speed.
- `Gzip` is using gzip compression, is the slowest, however should produce the best results. Maximum (Optimal) compression settings is chosen, as if you are going for gzip, you are probably considering compression as your top priority.


## Appending to Files

Appending to files is easy, however it's worth keeping in mind that *row groups are immutable* in parquet file, therefore you need to create a new row group in a file you wish to append to. The following code snippet illustrates this:

```csharp
//write a file with a single row group
var id = new DataField<int>("id");
var ms = new MemoryStream();

using (var writer = new ParquetWriter(new Schema(id), ms))
{
   using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
   {
      rg.WriteColumn(new DataColumn(id, new int[] { 1, 2 }));
   }
}

//append to this file. Note that you cannot append to existing row group, therefore create a new one
ms.Position = 0;
using (var writer = new ParquetWriter(new Schema(id), ms, append: true))
{
   using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
   {
      rg.WriteColumn(new DataColumn(id, new int[] { 3, 4 }));
   }
}

//check that this file now contains two row groups and all the data is valid
ms.Position = 0;
using (var reader = new ParquetReader(ms))
{
   Assert.Equal(2, reader.RowGroupCount);

   using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(0))
   {
      Assert.Equal(2, rg.RowCount);
      Assert.Equal(new int[] { 1, 2 }, rg.ReadColumn(id).Data);
   }

   using (ParquetRowGroupReader rg = reader.OpenRowGroupReader(1))
   {
      Assert.Equal(2, rg.RowCount);
      Assert.Equal(new int[] { 3, 4 }, rg.ReadColumn(id).Data);
   }

}

```

Note that you have to specify that you are opening `ParquetWriter` in **append** mode in it's constructor explicitly - `new ParquetWriter(new Schema(id), ms, append: true)`. Doing so makes parquet.net open the file, find the file footer and delete it, rewinding current stream posiition to the end of actual data. Then, creating more row groups simply writes data to the file as usual, and `.Dispose()` on `ParquetWriter` generates a new file footer, writes it to the file and closes down the stream.

Please keep in mind that row groups are designed to hold a large amount of data (5'0000 rows on average) therefore try to find a large enough batch to append to the file. Do not treat parquet file as a row stream by creating a row group and placing 1-2 rows in it, because this will both increase file size massively and cause a huge performance degradation for a client reading such a file.