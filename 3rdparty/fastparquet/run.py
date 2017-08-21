from fastparquet import ParquetFile

pf = ParquetFile("C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\nested.parquet")

df = pf.to_pandas()

print(df)