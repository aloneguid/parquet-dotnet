..\tools\thrift-0.13.0.exe --gen netstd -out ..\src\ ..\src\Parquet\Thrift\parquet.thrift

# and then:

# 1. Remove [Serialization] and Silverlight
# 2. disable pragma warnings for public members