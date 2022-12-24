using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
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

            (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type is Thrift.ConvertedType.TIMESTAMP_MILLIS or Thrift.ConvertedType.TIMESTAMP_MICROS) ||

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
               return ReadAsInt64(reader, tse,(DateTimeOffset[])dest, offset);
            case Thrift.Type.INT96:
               return ReadAsInt96(reader, (DateTimeOffset[])dest, offset);
            default:
               throw new NotSupportedException();
         }
      }

      protected override DateTimeOffset ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         if (tse == null) return default;

         switch (tse.Type)
         {
            case Thrift.Type.INT32:
                return ReadAsInt32(reader);
            case Thrift.Type.INT64:
                return ReadAsInt64(reader, tse);
            case Thrift.Type.INT96:
               return new NanoTime(reader.ReadBytes(12), 0);
            default:
               throw new NotSupportedException();
         }
      }

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, ArrayView values, DataColumnStatistics statistics)
      {
         switch (tse.Type)
         {
            case Thrift.Type.INT32:
               WriteAsInt32(writer, values, statistics);
               break;
            case Thrift.Type.INT64:
               WriteAsInt64(writer, values, statistics);
               break;
            case Thrift.Type.INT96:
               WriteAsInt96(writer, values, statistics);
               break;
            default:
               throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");
         }
      }

      private static int ReadAsInt32(BinaryReader reader, DateTimeOffset[] dest, int offset)
      {
         int idx = offset;
         while (reader.BaseStream.Position + 4 <= reader.BaseStream.Length)
         {
            dest[idx++] = ReadAsInt32(reader);
         }

         return idx - offset;
      }

      private static DateTimeOffset ReadAsInt32(BinaryReader reader)
      {
         int iv = reader.ReadInt32();
         DateTimeOffset e = iv.FromUnixDays();
         return e;
      }

      private void WriteAsInt32(BinaryWriter writer, ArrayView values, DataColumnStatistics dataColumnStatistics)
      {
         foreach (DateTimeOffset dto in values.GetValuesAndReturnArray(dataColumnStatistics, this, this))
         {
            WriteAsInt32(writer, dto);
         }
      }

      private static void WriteAsInt32(BinaryWriter writer, DateTimeOffset dto)
      {
         int days = (int)dto.ToUnixDays();
         writer.Write(days);
      }

      private static DateTimeOffset ReadAsInt64(BinaryReader reader, Thrift.SchemaElement tse)
      {
         long lv = reader.ReadInt64();
         if(tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIMESTAMP_MICROS) {
             lv /= 1000;
         }

         return lv.FromUnixMilliseconds();
      }

      private static int ReadAsInt64(BinaryReader reader, Thrift.SchemaElement tse, DateTimeOffset[] dest, int offset)
      {
         int idx = offset;

         while (reader.BaseStream.Position + 8 <= reader.BaseStream.Length)
         {
            DateTimeOffset dto = ReadAsInt64(reader, tse);
            dest[idx++] = dto;
         }

         return idx - offset;
      }

      private void WriteAsInt64(BinaryWriter writer, ArrayView values, DataColumnStatistics dataColumnStatistics)
      {
         foreach (DateTimeOffset dto in values.GetValuesAndReturnArray(dataColumnStatistics, this, this))
         {
            WriteAsInt64(writer, dto);
         }
      }

      private static void WriteAsInt64(BinaryWriter writer, DateTimeOffset dto)
      {
         long unixTime = dto.ToUnixMilliseconds();
         writer.Write(unixTime);
      }

      private static void ReadAsInt96(BinaryReader reader, IList result)
      {
         while (reader.BaseStream.Position + 12 <= reader.BaseStream.Length)
         {
            result.Add(ReadAsInt96(reader));
         }
      }

      private static int ReadAsInt96(BinaryReader reader, DateTimeOffset[] dest, int offset)
      {
         int idx = offset;
         while (reader.BaseStream.Position + 12 <= reader.BaseStream.Length)
         {
            dest[idx++] = ReadAsInt96(reader);
         }
         return idx - offset;
      }

      private static DateTimeOffset ReadAsInt96(BinaryReader reader)
      {
         var nano = new NanoTime(reader.ReadBytes(12), 0);
         DateTimeOffset dt = nano;
         return dt;
      }

      private void WriteAsInt96(BinaryWriter writer, ArrayView values, DataColumnStatistics dataColumnStatistics)
      {
         foreach (DateTimeOffset dto in values.GetValuesAndReturnArray<DateTimeOffset>(dataColumnStatistics, this, this))
         {
            WriteAsInt96(writer, dto);
         }
      }

      private static void WriteAsInt96(BinaryWriter writer, DateTimeOffset dto)
      {
         var nano = new NanoTime(dto);
         nano.Write(writer);
      }

      public override byte[] PlainEncode(Thrift.SchemaElement tse, DateTimeOffset x)
      {
         using(var ms = new MemoryStream())
         {
            using(var writer = new BinaryWriter(ms))
            {
               switch (tse.Type)
               {
                  case Thrift.Type.INT32:
                     WriteAsInt32(writer, x);
                     break;
                  case Thrift.Type.INT64:
                     WriteAsInt64(writer, x);
                     break;
                  case Thrift.Type.INT96:
                     WriteAsInt96(writer, x);
                     break;
                  default:
                     throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");
               }
            }

            return ms.ToArray();
         }
      }

      public override object PlainDecode(Thrift.SchemaElement tse, byte[] encoded)
      {
         if (encoded == null) return null;

         using(var ms = new MemoryStream(encoded))
         {
            using (var reader = new BinaryReader(ms))
            {
               switch (tse.Type)
               {
                  case Thrift.Type.INT32:
                     return ReadAsInt32(reader);
                  case Thrift.Type.INT64:
                     return ReadAsInt64(reader, tse);
                  case Thrift.Type.INT96:
                     return ReadAsInt96(reader);
                  default:
                     throw new NotSupportedException();
               }
            }
         }
      }
   }
}
