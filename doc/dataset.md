# Working with DataSet

Internally Parquet represents data in columnar format, however most of the interaction workflows are easier with row-based data, therefore Parquet.Net transorms data on read to row-based format and transforms back to columnar when writing to Parquet.

## Creating a DataSet

The `DataSet` can be created by passing schema to it's constructor either as `Schema` object or a list of `Field` (see [declaring schema](schema.md) for more details):


```csharp

using Parquet;
using Parquet.Data;

var ds1 = new DataSet(
	new Schema()
	{
		new DataField<int>("id"),
		new DataField<string>("city"
	});

var ds2 = new DataSet(
	new DataField<int>("id"),
	new DataField<string>("city"));
```

`DataField` here is a subclass of `Field` which is designed to hold column data, and is the most common schema element.

As `DataSet` implements `IList<Row>` interface you can keep adding `Row` objects to it:

```csharp
var row = new Row(1, "London");
ds1.Add(row);
```

The row object contains a list of untyped elements (`System.Object`), however `DataSet` will validate the types according to it's schema when adding elements and throw an appropriate exception.

## Utilities

### Merging DataSets

Two datasets can be merged together, if they have idential schemas, you can use `.Merge()` method for this:

```csharp
DataSet ds1 = ...
DataSet ds2 = ...

ds1.Merge(ds2);
```

Merge method takes data from `ds2` and adds into `ds1`. Internally it's super-fast comparing to manually adding a set of `Row`s as it operates on columns instead of rows.

## Counts

DataSet contains a few `Count` properties which carry a different meaning:

- **Count** is an alias to **RowCount** and returns the number of rows dataset physically contains.
- **TotalRowCount** represetns the number of rows in the source file the DataSet was read from. It's only filled by `ParquetDataReader`.
- **ColumnCount** is number of columns in this dataset and is just an alias property which reads number of columns from the schema.

## File Metadata

Parquet supports file-wide metadata which are key-value strings. When reading a file you can access this metadata by calling to `DataSet.Metadata.Custom` property. Likewise, you can set this collection to your custom values before writing file.
