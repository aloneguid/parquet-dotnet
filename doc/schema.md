# Declaring Schema

Due to the fact that Parquet is s strong typed format you need to declare a schema before writing any data.

Schema can be defined by creating an instance of `Schema` class and passing a collection of `Field`. Various helper methods on both `DataSet` and `Schema` exist to simplify the schema declaration, but we are going to be more specific on this page.

There are several types of fields you can specify in your schema, and the most common is `DataField`. Data field is derived from the base abstract `Field` class (just like all the rest of the field types) and simply means in declares an actual data rather than an abstraction.

You can declare a `DataField` by specifying a column name and it's type in the constuctor, in one of two forms:

```csharp
var field = new DataField("id", DataType.Int32);

var field = new Field<int>("id");
```

The first one is more declarative and allows you to select data type from the `DataType` enumeration containing the list of types we actually support at the moment.

The second one is just a shortcut to `DataField` that allows you to use .NET Generics.

## Null Values

Declaring schema as above will allow you to add elements of type `int`, however null values are not allowed (you will get an exception when trying to add a null value to the `DataSet`). In order to allow nulls you need to declare them in schema explicitly:

```csharp
new Field<int?>("id");

new DataField("id", DataType.Int32, true);
```

This allows you to force the schema to be nullable, so you can add null values. In many cases having a nullable column is useful even if you are not using nulls at the moment, for instance when you will append to the file later and will have nulls.

> Note that `string` type is always nullable. Although `string` is an [immutable type](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/strings/) in CLR, it's still passed by reference, which can be assigned to `null` value.

## Repeatable fields

todo