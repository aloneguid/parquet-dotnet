using System;
using System.Collections;
using System.IO;
using Parquet.File.Values.Primitives;

namespace Parquet.Data.Concrete
{
   class DateTimeDataTypeHandler : BasicPrimitiveDataTypeHandler<DateTime>
   {
      public DateTimeDataTypeHandler() : base(DataType.DateTimeOffset, Thrift.Type.BYTE_ARRAY)
      {

      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return

            (tse.Type == Thrift.Type.INT96 && formatOptions.TreatBigIntegersAsDates) || //Impala

            (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIMESTAMP_MILLIS) ||

            (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DATE);
      }

      protected override DateTime ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         throw new NotSupportedException("this stub should never be called");
      }
   }
}
