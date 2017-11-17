using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;

namespace Parquet.Data
{
   class IntervalDataType : BasicPrimitiveDataType<Interval>
   {
      public IntervalDataType() : base(DataType.Interval, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL)
      {

      }

      public override void CreateThrift(SchemaElement se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         base.CreateThrift(se, parent, container);

         //set type length to 12
         Thrift.SchemaElement tse = container.Last();
         tse.Type_length = 12;
      }


      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         IList result = CreateEmptyList(tse.IsNullable(), 0);
         int typeLength = tse.Type_length;
         if (typeLength == 0) return result;

         while(reader.BaseStream.Position + 12 <= reader.BaseStream.Length)
         {
            // assume this is the number of months / days / millis offset from the Julian calendar
            int months = reader.ReadInt32();
            int days = reader.ReadInt32();
            int millis = reader.ReadInt32();

            result.Add(new Interval(months, days, millis));
         }

         return result;
      }

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         foreach(Interval interval in values)
         {
            writer.Write(interval.Months);
            writer.Write(interval.Days);
            writer.Write(interval.Millis);
         }
      }
   }
}
