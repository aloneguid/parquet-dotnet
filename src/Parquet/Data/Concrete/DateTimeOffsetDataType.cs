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
   class DateTimeOffsetDataType : BasicPrimitiveDataType<DateTimeOffset>
   {
      public DateTimeOffsetDataType() : base(DataType.DateTimeOffset, Thrift.Type.INT96)
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
         if(se is DateTimeDataField dse)
         {
            switch(dse.DateTimeFormat)
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

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         IList result = CreateEmptyList(tse.IsNullable(), false, 0);

         switch(tse.Type)
         {
            case Thrift.Type.INT32:
               ReadAsInt32(reader, result);
               break;
            case Thrift.Type.INT64:
               ReadAsInt64(reader, result);
               break;
            case Thrift.Type.INT96:
               ReadAsInt96(reader, result);
               break;
            default:
               throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");
         }

         return result;
      }

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         switch(tse.Type)
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

      private void ReadAsInt32(BinaryReader reader, IList result)
      {
         while(reader.BaseStream.Position + 4 <= reader.BaseStream.Length)
         {
            int iv = reader.ReadInt32();
            result.Add(new DateTimeOffset(iv.FromUnixTime(), TimeSpan.Zero));
         }
      }

      private void WriteAsInt32(BinaryWriter writer, IList values)
      {
         foreach (DateTimeOffset dto in values)
         {
            int days = (int)dto.ToUnixDays();
            writer.Write(days + 1);
         }
      }

      private void ReadAsInt64(BinaryReader reader, IList result)
      {
         while (reader.BaseStream.Position + 8 <= reader.BaseStream.Length)
         {
            long lv = reader.ReadInt64();
            result.Add((DateTimeOffset)(lv.FromUnixTime()));
         }
      }

      private void WriteAsInt64(BinaryWriter writer, IList values)
      {
         foreach (DateTimeOffset dto in values)
         {
            long unixTime = dto.ToUnixTime();
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

      private void WriteAsInt96(BinaryWriter writer, IList values)
      {
         foreach(DateTimeOffset dto in values)
         {
            var nano = new NanoTime(dto);
            nano.Write(writer);
         }
      }
   }
}
