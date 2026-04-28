# parquet-xtract

A small showcase utility that demonstrates how to extract relational database to flat parquet files. This seems to be a popular use case for interfacing with legacy systems.

This utility is not production ready, not supported and is only created as a proof of concept.

This utility focuses on speed and very low memory footprint, but this might not be achieved yet.

Current database support:
- MSSQL
- Postgres

## Using

This can be installed as a [dotnet tool](https://www.nuget.org/packages/parquet-xtract) at the moment. Shouldn't be a problem to publish as self-contained executable if required.

The shortest working example, which connects to a Postgres database and extracts all tables to parquet files in the current directory, is:

```bash
parquet-xtract -s "Your connection string here" -d postgres
```

For sample MSSQL connection strings, see [here](https://www.connectionstrings.com/sql-server/) or for Postgres, see [here](https://www.connectionstrings.com/postgresql/).