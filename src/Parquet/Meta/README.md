To regenerate Thrift wrapper:

1. Grab the [latest parquet.thrift](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift).
2. Go to `/scripts` in the root of this repo.
3. Run `..\tools\thrift-0.16.0.exe --gen xml:no_namespaces,no_default_ns -out ..\src\Parquet\Meta\ ..\src\Parquet\Meta\parquet.thrift`. This will re-generate xml file in `scripts/gen-xml` folder. Move it under this (Meta) folder and rename it to `parquet.xml`.
4. Run `dotnet run thriftgen.cs` which will re-generate `Parquet.cs` in this (Meta) folder.