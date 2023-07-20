# Declaring Schema

Parquet is a format that stores data in a structured way. It has different types for different kinds of data, like numbers, strings, dates and so on. This means that you have to tell Parquet what type each column of your data is before you can write it to a file. This is called declaring a schema. Declaring a schema helps Parquet to compress and read your data more efficiently.

Schema can be defined by creating an instance of `ParquetSchema` class and passing a collection of `Field`. Various helper methods on both `DataSet` and `ParquetSchema` exist to simplify the schema declaration, but we are going to be more specific on this page.

There are several types of fields you can specify in your schema, and the most common is `DataField`. `DataField` is derived from the base abstract `Field` class (just like all the rest of the field types) and simply means in declares an actual data rather than an abstraction.

You can declare a `DataField` by specifying a column name and it's type in the constructor, in one of two forms:

```csharp
var field = new DataField("id", DataType.Int32);

var field = new Field<int>("id");
```

The first one is more declarative and allows you to select data type from the `DataType` enumeration containing the list of types we actually support at the moment.

The second one is just a shortcut to `DataField` that allows you to use .NET Generics.

Then, there are specialised versions for `DataField` allowing you to specify more precise metadata about certain parquet data type, for instance `DecimalDataField` allows to specify precision and scale other than default values.

Non-data field wrap complex structures like list (`ListField`), map (`MapField`) and struct (`StructField`).

Full schema type hierarchy can be expressed as:

```mermaid
classDiagram
    Schema "1" o-- "1..*" Field
    Field <|-- DataField
    DataField <|-- DataF1eld~T~
    DataField <|-- DateTimeDataField
    DataField <|-- DecimalDataField
    DataField <|-- TimeSpanDataField
    
    Field <|-- ListField
    Field "1" --o "1" ListField: list item

    Field <|-- MapField
    Field "1" --o "1" MapField: key field
    Field "1" --o "1" MapField: value field

    Field <|-- StructField
    Field "1..*" --o "1" StructField: struct members


    class Schema {
        +List~Field~: Fields
    }

    class Field {
        +string Name
        +SchemaType SchemaType
        +FieldPath Path
    }

    class DataField {
        +Type ClrType
        +bool IsNullable
        +bool IsArray
    }

    class DataF1eld~T~{
        
    }

    class DateTimeDataField {
        +DateTimeFormat: DateTimeFormat
    }

    class DecimalDataField {
        +int Precision
        +int Scale
        +bool: ForceByteArrayEncoding
    }

    class TimeSpanDataField {
        +TimeSpanFormat: TimeSpanFormat
    }

    class ListField {
        +Field: Item
    }

    class MapField {
        +Field: Key
        +Field: Value
    }

    class StructField {
        +List~Field~: Fields
    }
```



## Null Values

Declaring schema as above will allow you to add elements of type `int`, however null values are not allowed (you will get an exception when trying to add a null value to the `DataSet`). In order to allow nulls you need to declare them in schema explicitly by specifying a nullable type:

```csharp
new DataField<int?>("id");
// or
new DataField("id", typeof(int?));
```

This allows you to force the schema to be nullable, so you can add null values. In many cases having a nullable column is useful even if you are not using nulls at the moment, for instance when you will append to the file later and will have nulls.

Nullable columns incur a slight performance and data size overhead, as parquet needs to store an additional nullable flag for each value.

> Note that `string` type is always nullable. Although `string` is an [immutable type](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/strings/) in CLR, it's still passed by reference, which can be assigned to `null` value.

## Dates

In the old times Parquet format didn't support dates, therefore people used to store dates as `int96` number. Because of backward compatibility issues we use this as the default date storage format.

If you need to override date format storage you can use `DateTimeDataField` instead of `DataField<DateTime>` which allows to specify precision, for example the following example lowers precision to only write date part of the date without time.

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

Since v4.2.3 variable-size decimal encoding [(variant 4)](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) is supported by the reader.

## Repeatable Fields (Arrays)

Repeatable fields are fields that contain an array of values instead of just one single value.

To declare a repeatable field in schema you need specify it as `IEnumerable<T>` where `T` is one of the types Parquet.Net supports. For example:

```csharp
var se = new DataField<IEnumerable<int>>("ids");
```

you can also specify that a field is repeatable by setting `isArray` in `DataField` constructor to `true`.

When writing to the field you can specify any value which derives from `IEnumerable<int>`, for instance

```csharp
ds.Add(1, new int[] { 1, 2, 3 });
```

When reading schema back, you can check if it's repeatable by calling to `.IsArray` property. 

### The Difference between Lists and Arrays

You can also declare a list (`ListField`) with one element, so the question is - why do you need arrays? If you can, prefer lists to arrays because:

- Lists are more flexible - arrays can only contain a primitive type, whereas lists can contain anything.
- Most big data platforms just default to lists.
- Schema evolution is not possible with arrays.
