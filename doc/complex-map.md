# Maps

Map in parquet format are similar to C# dictionaries i.e. they can map a key to a value. The perfect use case for them is when you cannot define a schema beforehand, or really just want a map of values in a column.

For instance, let's declare a schema with two columns:

- Row ID
- A map of integer to it's human representation

```csharp
var ds = new DataSet(
   new DataField<int>("id")
   new MapField("names",
      new DataField("key", DataType.Int32),
      new DataField("value", DataType.String)),
   );

ds.Add(1, new Dictionary<int, string>
{
   [1] = "one",
   [2] = "two",
   [3] = "three"
});
```

Map value can also be a complex type like a structure, for example:

```csharp
var ds = new DataSet(
   new DataField<int>("id"),
   new StructField("structure",
      new DataField<int>("id"),
      new MapField("map",
         new DataField<int>("key"),
         new DataField<string>("value")
   )));
ds.Add(1, new Row(1, new Dictionary<int, string>
{
   [1] = "one",
   [2] = "two",
   [3] = "three"
}));
```