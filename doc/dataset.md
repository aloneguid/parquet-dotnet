# Working with DataSet

Internally Parquet represents data in columnar format, however most of the interaction workflows are easier with row-based data, therefore Parquet.Net transorms data on read to row-based format and transforms back to columnar when writing to Parquet.

## Creating a DataSet

The `DataSet` can be created by passing schema to it's constructor either as `Schema` object or a list of `SchemaElement`:


```csharp

using Parquet;
using Parquet.Data;

var ds1 = new DataSet(
	new Schema()
	{
		new SchemaElement<int>("id"),
		new SchemaElement<string>("city"
	});

var ds2 = new DataSet(
	new SchemaElement<int>("id"),
	new SchemaElement<string>("city"));
```

As `DataSet` implements `IList<Row>` interface you can keep adding `Row` objects to it:

```csharp
var row = new Row(1, "London");
ds1.Add(row);
```

The row object contains a list of untyped elements (`System.Object`), however `DataSet` will validate the types according to it's schema when adding elements and throw an appropriate exception.

## Schema

Schema can be expressed as a set of `SchemaElement` instances. `SchemaElement` describes a column and has only two required attributes

- column name
- [column type](types.md)

### A special note on dates

In the old times Parquet format didn't support dates, therefore people used to store dates as `int96` number. Because of backward compatibility issues we use this as the default date storage format.

If you need to override date format storage you can use `DateTimeSchemaElement` instead of `SchemaElement<DateTimeOffset>` which allows to specify precision, for example the following example lowers precision to only write date part of the date without time.

```csharp
new DateTimeSchemaElement("date_col", DateTimeFormat.Date);
```

see `DateTimeFormat` enumeration for detailed explanation of available options.

## Counts

DataSet contains a few `Count` properties which carry a different meaning:

- **Count** is an alias to **RowCount** and returns the number of rows dataset physically contains.
- **TotalRowCount** represetns the number of rows in the source file the DataSet was read from. It's only filled by `ParquetDataReader`.
- **ColumnCount** is number of columns in this dataset and is just an alias property which reads number of columns from the schema.
