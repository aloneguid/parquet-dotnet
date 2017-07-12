# Working with DataSet

Internally Parquet represents data in columnar format, however most of the interaction workflows are easier with row-based data, therefore Parquet.Net transorms data on read to row-based format and transforms back to columnar when writing to Parquet.

## Creating a DataSet

> todo

## Schema

Schema can be expressed as a set of `SchemaElement` instances. `SchemaElement` describes a column and has only two required attributes

- column name
- column type

### Column type

A type can be one of the supported types:

- `int`
- `bool`
- `string`
- `float`
- `double`
- `DateTimeOffset`

Note that you cannot pass nullable types as a schema specification, because any column value is allowed to accept nulls.