using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;

namespace Parquet.Data.Concrete
{
   class IntervalDataTypeHandler : BasicPrimitiveDataTypeHandler<Interval>
   {
      public IntervalDataTypeHandler() : base(DataType.Interval, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL)
      {

      }

      public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         base.CreateThrift(se, parent, container);

         //set type length to 12
         Thrift.SchemaElement tse = container.Last();
         tse.Type_length = 12;
      }

      public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset)
      {
         int typeLength = tse.Type_length;
         if (typeLength == 0) return 0;
         int idx = offset;
         Interval[] tdest = (Interval[])dest;

         while (reader.BaseStream.Position + 12 <= reader.BaseStream.Length)
         {
            // assume this is the number of months / days / millis offset from the Julian calendar
            int months = reader.ReadInt32();
            int days = reader.ReadInt32();
            int millis = reader.ReadInt32();
            var e = new Interval(months, days, millis);

            tdest[idx++] = e;
         }

         return idx - offset;
      }

      protected override Interval ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         int months = reader.ReadInt32();
         int days = reader.ReadInt32();
         int millis = reader.ReadInt32();
         return new Interval(months, days, millis);
      }

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values, Thrift.Statistics statistics)
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
