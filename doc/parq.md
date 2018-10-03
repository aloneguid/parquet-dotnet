# PARQ (Global Tool)

Since v3.1 parquet repository includes an amazing [.NET Core Global Tool](https://docs.microsoft.com/en-us/dotnet/core/tools/global-tools) called **parq** which serves as a first class command-line client to perform various funtions on parquet files.

## Installing

Installing is super easy with *global tools*, just go to the terminal and type `dotnet tool install -g parq` and it's done. Note that you need to have at least **.NET Core 2.1 SDK** isntalled on your machine, which you probably have as a hero .NET developer.

## Commands

### Viewing Schema

To view schema type

```powershell
parq schema <path-to-file>
````

which produces an output similar to:

![Parq Schema](img/parq-schema.png)

### More Commands

They are coming soon, please leave your comments in the issue tracker in terms of what you would like to see next.