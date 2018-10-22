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
new DataField<int?>("id");

new DataField("id", DataType.Int32, true);
```

This allows you to force the schema to be nullable, so you can add null values. In many cases having a nullable column is useful even if you are not using nulls at the moment, for instance when you will append to the file later and will have nulls.

> Note that `string` type is always nullable. Although `string` is an [immutable type](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/strings/) in CLR, it's still passed by reference, which can be assigned to `null` value.

## Dates

In the old times Parquet format didn't support dates, therefore people used to store dates as `int96` number. Because of backward compatibility issues we use this as the default date storage format.

If you need to override date format storage you can use `DateTimeDataField` instead of `DataField<DateTimeOffset>` which allows to specify precision, for example the following example lowers precision to only write date part of the date without time.

```csharp
new DateTimeDataField("date_col", DateTimeFormat.Date);
```

see `DateTimeFormat` enumeration for detailed explanation of available options.

## Decimals

Writing a decimal by default uses precision 38 and scele 18, however you can set different precision and schema by using `DecimalDataField` schema element (see constructor for options).

Note that AWS Athena, Impala and possibly other systems do not conform to Parquet specifications when reading decimal fields. If this is the case, you must use `DecimalDataField` explicitly and set `forceByteArrayEncoding` to `true`.

For instance:

```csharp
new DecimalDataField("decInt32", 4, 1); // uses precision 4 and scale 1

new DecimalDataField("decMinus", 10, 2, true); // uses precision 10 and scale 2, and enforces legacy decimal encoding that Impala understands
```

## Repeatable Fields

Parquet.Net supports repeatable fields i.e. fields that contain an array of values instead of just one single value.

To declare a repeatable field in schema you need specify it as `IEnumerable<T>` where `T` is one of the types Parquet.Net supports. For example:

```csharp
var se = new DataField<IEnumerable<int>>("ids");
```

you can also specify that a field is repeatable by setting `isArray` in `DataField` constructor to `true`.

When writing to the field you can specify any value which derives from `IEnumerable<int>`, for instance

```csharp
ds.Add(1, new int[] { 1, 2, 3 });
```

When reading schema back, you can check if it's repeatable by calling to `.IsArray` member. 