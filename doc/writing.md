# Writing Data

You can write data by constructing an instance of [ParquetWriter class](../src/Parquet/ParquetWriter.cs) or using one of the helper classes.

Writing files is a multi stage process, giving you the full flexibility on what exactly to write to it:

1. Create `ParquetWriter` passing it a *file schema* and a *writeable stream*. You should have declared file schema beforehand.
2. Create a row group writer by calling to `writer.CreateRowGroup(rowSize)`.
3. Keep calling `.Write()` by passing the data columns with data you want to write. Note that the order of data columns you are writing must match the order of data fields declared in the schema.

## Appending to Files

todo: is implemented, need documenting