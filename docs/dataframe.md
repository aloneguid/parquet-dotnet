# DataFrame Support

Since v4.8 support for [`Microsoft.Data.Analysis`](https://www.nuget.org/packages/Microsoft.Data.Analysis) was added. 

This is an early draft and you are welcome to create issues/discussions around it.

## What's Supported?

Due to `DataFrame` being in general less functional than Parquet, only primitive (atomic) columns are supported at the moment. If `DataFrame` supports more functionality in future, this integration can be extended.

In addition to this, structs are also supported, as long as they do not contain any maps or lists. Structs are flattened into separate flat columns.

## Reading

```csharp
DataFrame df = await fs.ReadParquetAsDataFrameAsync();
```

## Related Issues

- https://github.com/dotnet/machinelearning/issues/6144
  - https://github.com/dotnet/machinelearning/issues/6088
  - https://github.com/dotnet/machinelearning/issues/5972