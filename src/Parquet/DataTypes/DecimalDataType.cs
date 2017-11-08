using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class DecimalDataType : BasicDataType<decimal>
   {
      public DecimalDataType() : base(Thrift.Type.FIXED_LEN_BYTE_ARRAY, ConvertedType.DECIMAL)
      {
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return

            tse.__isset.converted_type && tse.Converted_type == ConvertedType.DECIMAL &&

            (
               tse.Type == Thrift.Type.FIXED_LEN_BYTE_ARRAY ||
               tse.Type == Thrift.Type.INT32 ||
               tse.Type == Thrift.Type.INT64
            );
      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.Decimal, parent);
      }
   }
}
