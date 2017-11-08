using System.Collections.Generic;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class StringDataType : BasicDataType<string>
   {
      public StringDataType() : base(Thrift.Type.BYTE_ARRAY)
      {
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.type &&
            tse.Type == Thrift.Type.BYTE_ARRAY &&
            (
               (tse.__isset.converted_type && tse.Converted_type == ConvertedType.UTF8) ||
               formatOptions.TreatByteArrayAsString
            );
      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.String, parent);
      }
   }
}
