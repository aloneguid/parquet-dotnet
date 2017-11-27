# Structures

In addition to [specifying basic types](schema.md), schema can contain complex structures. Let's say you have a table defined as:

```json
{
   "name": "Ivan",
   "addressLine1": "Woods",
   "addressPostcode": "postcode"
}
```

to save this data in parquet, you would define the following schema:

```csharp
var schema = new Schema(
   new DataField<string>("name"),
   new DataField<string>("addressLine1"),
   new DataField<string>("addressPostcode"));
```

Although that's not pretty, it works perfectly fine. However, another way to express this data would be to have a separate **address** entity:

```json
{
   "name": "Ivan",
   "address": {
      "line1": "Woods",
      "postcode": "postcode"
   }
}
```

The corresponding schema definition is:

```csharp
var schema = new Schema(
   new DataField<string>("name"),
   new StructField("address",
      new DataField<string>("line1"),
      new DataField<string>("postcode")
);
```

Let's write a file out with one record to `c:\tmp\nested.parquet`:

```csharp
var ds = new DataSet(schema);

//add a new row
ds.Add("Ivan", new Row("Woods", "postcode"));

//write to file
ParquetWriter.WriteFile(ds, "c:\\tmp\\nested.parquet");
```

**name** element is expressed as a simple value, whereas because **address** is a nested structure, you have to specify a `Row` with values corresponding to the rows in that structure.

## Lists of Structures

Whenever you need to specify a list of structures instead of just one single value, you can [use lists](complex-list.md) and include the structure as a list element, for example:

```csharp
var ds = new DataSet(
   new DataField<int>("id"),
   new ListField("cities",
   new StructField("element",
      new DataField<string>("name"),
      new DataField<string>("country"))));

ds.Add(1, new[] { new Row("London", "UK"), new Row("New York", "US") });
```

### Nesting Structures

You can nest structures into structures on any level, for example:

```csharp
var ds = new DataSet(
   new DataField<string>("name"),
   new StructField("address",
      new DataField<string>("name"),
      new StructField("lines",
         new DataField<string>("line1"),
         new DataField<string>("line2"))));

ds.Add("Ivan", new Row("Primary", new Row("line1", "line2")));
```

Nesting level is virtually unlimited.