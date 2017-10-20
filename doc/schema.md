# Declaring Schema

Due to the fact that Parquet is s strong typed format you need to declare a schema before writing any data.

Schema can be defined by creating an instance of `Schema` class and passing a collection of `SchemaElement`. Various helper methods on both `DataSet` and `Schema` exist to simplify the schema declaration, but we are going to be more specific on this page.

You can declare the `SchemaElement` by specifying a column name and it's type in the constuctor, in one of two forms:

```csharp
var element = new SchemaElement<int>("id");

var element new SchemaElement("id", typeof(int));
```

## Null Values

Declaring schema as above will allow you to add elements of type `int`, however null values are not allowed (you will get an exception when trying to add a null value to the `DataSet`).

In Parquet.Net up to v1.3 the library would allow you to add null values regardless, however as this leads to many errors and not exactly ergonomic, since v1.4 you have to specify manually that a schema element can have nulls:

```csharp
new SchemaElement<int?>("id");

new SchemaElement("id", typeof(int?));
```

This allows you to force the schema to be nullable, so you can add null values. In many cases having a nullable column is useful even if you are not using nulls at the moment, for instance when you will append to the file later and will have nulls.