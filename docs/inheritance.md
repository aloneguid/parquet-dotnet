# Serialization of Inherited Properties

Support for inherited properties was [added in v4.2](https://github.com/aloneguid/parquet-dotnet/pull/202).

## Default Behavior

By default, inherited properties are not included in serialization.  Only top-level properties will be picked up by the `SchemaReflector`, and the `Serializer` will not write inherited properties as parquet columns as a result.

When serializing an array of objects, you're able to pass in a `Schema`, which defines the columns that will end up in the serialized parquet files.  This argument is optional, and defaults to a Schema reflection of the object type you're serializing.  
For example, given a `SimpleStructure`, the default Schema object would be created like this

```csharp
public class SimpleStructure
{
   public int Id { get; set; }
   public string Name { get; set; }
}
```
```csharp
Schema schema = SchemaReflector.Reflect<SimpleStructure>();
```

This `Schema` only contains top-level properties.  So, a more complex structure that inherits the `Id` and `Name` properties from the `SimpleStructure` wouldn't include those properties in the serialized parquet by default.

```csharp
public class ComplexStructure : SimpleStructure
{
  public string Description { get; set; }
}
```

If we were to serialize an instance of a `ComplexStructure` with the default `Schema`, the output would _only_ have the `Description` property, regardless of `Id` and `Name` also being set.

```csharp
Schema defaultSchema = SchemaReflector.Reflect<ComplexStructure>();

defaultSchema.GetDataFields(); 
// => { PropertyInfo(Name="Description") }
```

## How to Include Inherited Properties

In order to include inherited properties when serializing an object, we simply use a different `Schema` reflection method.

```csharp 
Schema schemaWithInheritedProperties = SchemaReflector.ReflectWithInheritedProperties<ComplexStructure>();

schemaWithInheritedProperties.GetDataFields(); 
/**
 * => { 
 *     PropertyInfo(Name="Id"),
 *     PropertyInfo(Name="Name"),
 *     PropertyInfo(Name="Description")
 * }
 */
```
