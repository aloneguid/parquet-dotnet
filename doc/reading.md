# Reading Data

You can read the data by constructing an instance of [ParquetReader class](../src/Parquet/ParquetReader.cs) or using one of the helper classes. In the simplest case, to read a file located in `c:\data\input.parquet` you would write the following code:

```csharp
using System.IO;
using Parquet;
using Parquet.Data;

using(Stream fs = File.OpenRead("c:\\data\\input.parquet"))
{
	using(var reader = new ParquetReader(fs))
	{
		DataSet ds = reader.Read();
	}
}
```

[DataSet](../src/Parquet/Data/DataSet.cs) is a rich structure representing the data from the input file.

You can also do the same thing in one line with a helper method

```csharp
DataSet ds = ParquetReader.ReadFile("c:\\data\\input.parquet")
```

Another helper method is for reading from stream:

```csharp
using(Stream fs = File.OpenRead("c:\\data\\input.parquet"))
{
	DataSet ds = ParquetReader.Read(fs);
}
```

## Reading parts of file

Parquet.Net supports reading portions of files using offset and count properties. In order to do that you need to pass `ReaderOptions` and specify the desired parameters. Every `Read` method supports those as optional parameters. 

For example, to read `input.parquet` from rows 10 to 15 use the following code:

```csharp
var options = new ReaderOptions { Offset = 10, Count = 5};
DataSet ds = ParquetReader.ReadFile("c:\\data\\input.parquet", null, options);
```

## Using format options

When reading, Parquet.Net uses some defaults specified in [ParquetOptions.cs](../src/Parquet/ParquetOptions.cs), however you can override them by passing to a `Read` method.

For example, to force the reader to treat byte arrays as strings use the following code:

```csharp
var options = new ParquetOptions { TreatByteArrayAsString = true };
DataSet ds = ParquetReader.ReadFile("c:\\data\\input.parquet", options, null);
```
