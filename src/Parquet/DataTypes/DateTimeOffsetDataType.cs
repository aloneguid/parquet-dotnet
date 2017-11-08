using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class DateTimeOffsetDataType : BasicDataType<DateTimeOffset>
   {
      public DateTimeOffsetDataType() : base(Thrift.Type.BYTE_ARRAY)
      {

      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return

            tse.Type == Thrift.Type.INT96 || //Impala

            (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type == ConvertedType.TIMESTAMP_MILLIS) ||

            (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type && tse.Converted_type == ConvertedType.DATE);
      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.DateTimeOffset, parent);
      }
   }
}
