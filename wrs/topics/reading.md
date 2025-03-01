# Reading data

You can read the data by constructing an instance of [ParquetReader class](%src_base%/ParquetReader.cs) or using one of the static helper methods on the `ParquetReader` class, like `ParquetReader.OpenFromFile()`.

Reading files is a multi-stage process, giving you the full flexibility on what exactly to read from it:

1. Create `ParquetReader` from a source stream or open it with any utility method. Once the reader is open you can immediately access file schema and other global file options like key-value metadata and number of row groups.
2. Open `RowGroupReader` by calling to `reader.OpenRowGroupReader(groupIndex)`. This class also exposes general row group properties like row count.
3. Call `.Read()` on row group reader passing the `DataField` schema definition you wish to read.
4. Returned `DataColumn` contains the column data. Important thing to note here is we automatically merge data and definition levels of the column so that `.Data` member of type `System.Array` contains actual usable column data. Note that we do not process *repetition levels* if the column is a part of a more complex structure, and you have to use them appropriately. Simple data columns do not contain *repetition levels*.

It's worth noting that *repetition levels* are only used for complex data types like arrays, list and maps. Processing them automatically would add an enormous performance overhead, therefore we are leaving it up to you to decide how to use them.

## Using format options

When reading, Parquet.Net uses some defaults specified in [ParquetOptions.cs](%src_base%/ParquetOptions.cs), however you can override them by passing to a `ParquetReader` constructor.

For example, to force the reader to treat byte arrays as strings use the following code:

```C#
var options = new ParquetOptions { TreatByteArrayAsString = true };
var reader = await ParquetReader.CreateAsync(stream, options);
```

## Metadata

To read custom metadata you can access the `CustomMetadata` property on `ParquetReader`:

```C#
using (var reader = await ParquetReader.CreateAsync(ms)) {
   Assert.Equal("value1", reader.CustomMetadata["key1"]);
   Assert.Equal("value2", reader.CustomMetadata["key2"]);
}
```

## Statistics

You can read column statistics of a particular row group at zero cost by calling to `GetStatistics(DataField field)`.

## Parallelism

File stream are generally not compatible with parallel processing. You can, however, open file stream per parallel thread i.e. your `Parallel.For` should perform file opening operation. Or you can introduce a lock on file read, depends on what works better for you. I might state the obvious here, but asynchronous and parallel are not the same thing.

Here is an example of reading a file in parallel, where a unit of paralellism is a row group:

```C#
var reader = await reader.CreateAsync(path);
var count = reader.RowGroupCount;

await Parallel.ForAsync(0, count,
    async (i, cancellationToken) => {
        // create an instance of a row group reader for each group
        using (var gr = await ParquetReader.CreateAsync(path)) {
            using (var rgr = gr.OpenRowGroupReader(i)) {
              // process the row group ...
            }
        }
    }
);
```