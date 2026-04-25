# XTract

A small showcase utility that demonstrates how to extract relational database to flat parquet files. This seems to be a popular use case for data analytics and machine learning workflows.

This utility focuses on speed and very low memory footprint.

The first version only supports Microsoft SQL Server.

This utility is not production ready, not supported and is only created as a proof of concept.

## Using

This can be installed as a [dotnet tool](https://www.nuget.org/packages/Parquet.XTract) at the moment. Shouldn't be a problem to publish as self-contained executable if required.

[Sample MSSQL Connection strings](https://www.connectionstrings.com/sql-server/).