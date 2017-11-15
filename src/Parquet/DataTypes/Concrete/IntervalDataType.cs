using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;

namespace Parquet.DataTypes
{
   class IntervalDataType : BasicPrimitiveDataType<Interval>
   {
      public IntervalDataType() : base(DataType.Interval, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL)
      {

      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         IList result = CreateEmptyList(tse, formatOptions, 0);
         int typeLength = tse.Type_length;
         if (typeLength == 0) return result;

         while(reader.BaseStream.Position < reader.BaseStream.Length)
         {
            // assume this is the number of months / days / millis offset from the Julian calendar
            //todo: optimize allocations
            byte[] months = reader.ReadBytes(4);
            byte[] days = reader.ReadBytes(4);
            byte[] millis = reader.ReadBytes(4);

            result.Add(new Interval(
               BitConverter.ToInt32(months, 0),
               BitConverter.ToInt32(days, 0),
               BitConverter.ToInt32(millis, 0)));

         }

         return result;
      }
   }
}
