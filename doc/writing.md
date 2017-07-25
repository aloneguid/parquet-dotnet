# Writing Data

You can write data by constructing an instance of [ParquetWriter class](../src/Parquet/ParquetWriter.cs) or using one of the helper classes. In the simplest case, to write a sample dataset to `c:\data\output.parquet` you would write the following code:

```csharp
using System.IO;
using Parquet;
using Parquet.Data;

var ds = new DataSet(
	new SchemaElement<int>("id"),
	new SchemaElement<string>("city")
);

ds.Add(1, "London");
ds.Add(2, "Derby");

using(Stream fileStream = File.OpenWrite("c:\\data\\output.parquet"))
{
	using(var writer = new ParquetWriter(fileStream))
	{
		writer.Write(ds);
	}
}

```

[DataSet](../src/Parquet/Data/DataSet.cs) is a rich structure representing the data and is used extensively in Parquet.Net.

You can also do the same thing simpler with a helper method

```csharp
using(Stream fileStream = File.OpenWrite("c:\\data\\output.parquet"))
{
	ParquetWriter.Write(ds, fileStream);
}

```

## Settings writer options

You can set some writer options by passing [WriterOptions](../src/Parquet/WriterOptions.cs) instance to `Write` methods, which at the moment are:

- **RowGroupsSize** size of a row group when writing. Parquet internally can page file in row groups which can decrease the amount of RAM when making queries over the file in systems like Apache Spark. By default we set row group size to 5000 rows.