# Complex Types

Please read [getting started with Parquet](parquet-getting-started.md) to better understand Parquet internals.

## Arrays

Arrays *aka repeatable fields* is a basis for understanding how more complex data structures work in Parquet.

`DataColumn` in Parquet can contain not just a single but multiple values. Sometimes they are called repeated fields (because the data type value repeats) or arrays. In order to create a schema for a repeatable field, let's say of type `int` you could use one of two forms:

```csharp
var field = new DataField<IEnumerable<int>>("items");
```
or
```csharp
var field= new DataField("items", DataType.Int32, isArray: true);
```

Apparently to check if the field is repeated you can always check `.IsArray` boolean flag.

Array column is also a usual instance of the `DataColumm` class, however in order to populate it you need to pass **repetition levels**. Repetition levels specify *at which level array starts* (please read more details on this in the link above). 

### Example

Let's say you have a following array of integer arrays:

```
[1 2 3]
[4 5]
[6 7 8 9]
```

This can be represented as:

```
values:             [1 2 3 4 5 6 7 8 9]
repetition levels:  [0 1 1 0 1 0 1 1 1]
```

Where `0` means that this is a start of an array and `1` - it's a value continuation.

To represent this in C# code:

```csharp
var field = new DataField<IEnumerable<int>>("items");
var column = new DataColumn(
   field,
   new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 },
   new int[] { 0, 1, 1, 0, 1, 0, 1, 1, 1 });
```

### Empty Arrays

Empty arrays can be represented by simply having no element in them. For instance

```
[1 2]
[]
[3 4]
```

Goes into following:

```
values:             [1 2 null 3 4]
repetition levels:  [0 1 0    0 1]
```

> Note that anything other than plain columns add a performance overhead due to obvious reasons for the need to pack and unpack data structures.

## Structures

todo

## Lists

todo

## Maps

##
