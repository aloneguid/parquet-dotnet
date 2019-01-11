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
   class DateTimeOffsetDataTypeHandler : BasicPrimitiveDataTypeHandler<DateTimeOffset>
   {
      public DateTimeOffsetDataTypeHandler() : base(DataType.DateTimeOffset, Thrift.Type.INT96)
      {

      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return

            (tse.Type == Thrift.Type.INT96 && formatOptions.TreatBigIntegersAsDates) || //Impala

            (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIMESTAMP_MILLIS) ||

            (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DATE);
      }

      public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         base.CreateThrift(se, parent, container);

         //modify annotations
         Thrift.SchemaElement tse = container.Last();
         if (se is DateTimeDataField dse)
         {
            switch (dse.DateTimeFormat)
            {
               case DateTimeFormat.DateAndTime:
                  tse.Type = Thrift.Type.INT64;
                  tse.Converted_type = Thrift.ConvertedType.TIMESTAMP_MILLIS;
                  break;
               case DateTimeFormat.Date:
                  tse.Type = Thrift.Type.INT32;
                  tse.Converted_type = Thrift.ConvertedType.DATE;
                  break;

               //other cases are just default
            }
         }
         else
         {
            //default annotation is fine
         }

      }

      public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset)
      {
         switch (tse.Type)
         {
            case Thrift.Type.INT32:
               return ReadAsInt32(reader, (DateTimeOffset[])dest, offset);
            case Thrift.Type.INT64:
               return ReadAsInt64(reader, (DateTimeOffset[])dest, offset);
            case Thrift.Type.INT96:
               return ReadAsInt96(reader, (DateTimeOffset[])dest, offset);
            default:
               throw new NotSupportedException();
         }
      }

      protected override DateTimeOffset ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         switch (tse.Type)
         {
            case Thrift.Type.INT32:
               int iv = reader.ReadInt32();
               return iv.FromUnixDays();
            case Thrift.Type.INT64:
               long lv = reader.ReadInt64();
               return lv.FromUnixMilliseconds();
            case Thrift.Type.INT96:
               return new NanoTime(reader.ReadBytes(12), 0);
            default:
               throw new NotSupportedException();
         }
      }

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values, Thrift.Statistics statistics)
      {
         switch (tse.Type)
         {
            case Thrift.Type.INT32:
               WriteAsInt32(writer, values);
               break;
            case Thrift.Type.INT64:
               WriteAsInt64(writer, values);
               break;
            case Thrift.Type.INT96:
               WriteAsInt96(writer, values);
               break;
            default:
               throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");
         }
      }

      private int ReadAsInt32(BinaryReader reader, DateTimeOffset[] dest, int offset)
      {
         int idx = offset;
         while (reader.BaseStream.Position + 4 <= reader.BaseStream.Length)
         {
            int iv = reader.ReadInt32();
            DateTimeOffset e = iv.FromUnixDays();
            dest[idx++] = e;
         }

         return idx - offset;
      }

      private void WriteAsInt32(BinaryWriter writer, IList values)
      {
         foreach (DateTimeOffset dto in values)
         {
            int days = (int)dto.ToUnixDays();
            writer.Write(days);
         }
      }

      private void ReadAsInt64(BinaryReader reader, IList result)
      {
         while (reader.BaseStream.Position + 8 <= reader.BaseStream.Length)
         {
            long lv = reader.ReadInt64();
            result.Add(lv.FromUnixMilliseconds());
         }
      }

      private int ReadAsInt64(BinaryReader reader, DateTimeOffset[] dest, int offset)
      {
         int idx = offset;

         while (reader.BaseStream.Position + 8 <= reader.BaseStream.Length)
         {
            long lv = reader.ReadInt64();
            DateTimeOffset dto = lv.FromUnixMilliseconds();
            dest[idx++] = dto;
         }

         return idx - offset;
      }

      private void WriteAsInt64(BinaryWriter writer, IList values)
      {
         foreach (DateTimeOffset dto in values)
         {
            long unixTime = dto.ToUnixMilliseconds();
            writer.Write(unixTime);
         }
      }

      private void ReadAsInt96(BinaryReader reader, IList result)
      {
         while (reader.BaseStream.Position + 12 <= reader.BaseStream.Length)
         {
            var nano = new NanoTime(reader.ReadBytes(12), 0);
            DateTimeOffset dt = nano;
            result.Add(dt);
         }
      }

      private int ReadAsInt96(BinaryReader reader, DateTimeOffset[] dest, int offset)
      {
         int idx = offset;
         while (reader.BaseStream.Position + 12 <= reader.BaseStream.Length)
         {
            var nano = new NanoTime(reader.ReadBytes(12), 0);
            DateTimeOffset dt = nano;
            dest[idx++] = dt;
         }
         return idx - offset;
      }

      private void WriteAsInt96(BinaryWriter writer, IList values)
      {
         foreach (DateTimeOffset dto in values)
         {
            var nano = new NanoTime(dto);
            nano.Write(writer);
         }
      }
   }
}
