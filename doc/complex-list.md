# Lists

Lists provide a better alternative to [repeatable fields](schema.md#repeatable-fields) so that they can store any data, unlike a repeatable field which can only repeat primitive data type values like an array does. You can put any type inside a list, for instance a structure:

```csharp
var ds = new DataSet(
   new DataField<int>("id"),
   new ListField("cities",
   new StructField("element",
      new DataField<string>("name"),
      new DataField<string>("country"))));

ds.Add(1, new[] { new Row("London", "UK"), new Row("New York", "US") });
```

Everything else, including [primitive types](schema.md), [maps](complex-map.md), [structures](complex-struct) or even lists themselves can be used as a list member.