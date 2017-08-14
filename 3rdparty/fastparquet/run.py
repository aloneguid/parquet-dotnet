from fastparquet import ParquetFile

pf = ParquetFile("C:\dev\parquet-dotnet\src\Parquet.Test\data\customer.impala.parquet")

df = pf.to_pandas()

#print(df)