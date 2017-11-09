using System.Collections.Generic;
using Parquet.Data;

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
               (tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.UTF8) ||
               formatOptions.TreatByteArrayAsString
            );
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.String, parent);
      }
   }
}
