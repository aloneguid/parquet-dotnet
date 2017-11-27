# Maps

Maps are represented as `IDictionary<TKey, TValue>` where both key and value must be a simple type (no structures are supported):

```csharp
var ds = new DataSet(
   new SchemaElement<IDictionary<int, string>>("names"),
   new DataField<int>("id"));

ds.Add(new Dictionary<int, string>
{
   [1] = "one",
   [2] = "two",
   [3] = "three"
}, 1);
```