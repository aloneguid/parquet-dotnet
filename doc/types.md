# Supported Types

Parquet.Net tries to fit natively into .NET environment and map built-in CLR types to Parquet type system. The following table lists the types supported at the moment. These are the types you can specify in the constructor of `SchemaElement<TType>`:

| CLR Type |  Parquet Type | Parquet Annotation |
|----------|---------------|--------------------|
|System.Byte (byte)|INT32|UNIT_8|
|System.SByte (sbyte)|INT32|UINT_8|
|byte[]|BYTE_ARRAY||
|System.Int16 (short)|INT_32|INT_16|
|System.UInt16 (ushort)|INT_32|UINT_16|
|System.Int32 (int)|INT32||
|System.Boolean (bool)|BOOLEAN||
|System.String (string)|BYTE_ARRAY|UTF8|
|System.Single (float)|FLOAT||
|System.Int64 (long)|INT64||
|System.Double (double)|DOUBLE||
|System.Decimal (decimal)|BYTE_ARRAY|DECIMAL|
|System.DateTimeOffset (DateTimeOffset)|INT96|
|System.DateTime (DateTime)|INT96||