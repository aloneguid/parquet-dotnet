using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class DecimalDataType : BasicDataType<decimal>
   {
      public DecimalDataType() : base(Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.DECIMAL)
      {
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return

            tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DECIMAL &&

            (
               tse.Type == Thrift.Type.FIXED_LEN_BYTE_ARRAY ||
               tse.Type == Thrift.Type.INT32 ||
               tse.Type == Thrift.Type.INT64
            );
      }

      public override IList Read(BinaryReader reader)
      {
         throw new NotImplementedException();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Decimal, parent);
      }
   }
}
