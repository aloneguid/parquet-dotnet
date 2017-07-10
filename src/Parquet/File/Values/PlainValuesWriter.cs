using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using TType = Parquet.Thrift.Type;
using SType = System.Type;
using Parquet.Data;

namespace Parquet.File.Values
{
   //see https://github.com/Parquet/parquet-format/blob/master/Encodings.md#plain-plain--0
   class PlainValuesWriter : IValuesWriter
   {
      private ParquetOptions _options;
      private static readonly System.Text.Encoding UTF8 = System.Text.Encoding.UTF8;

      public PlainValuesWriter(ParquetOptions options)
      {
         _options = options;
      }

      public void Write(BinaryWriter writer, SchemaElement schema, IList data)
      {
         switch (schema.Thrift.Type)
         {
            case TType.BOOLEAN:
               WriteBoolean(writer, schema, data);
               break;

            case TType.INT32:
               WriteInt32(writer, schema, data);
               break;

            case TType.FLOAT:
               WriteFloat(writer, schema, data);
               break;

            case TType.INT64:
               WriteLong(writer, schema, data);
               break;

            case TType.DOUBLE:
               WriteDouble(writer, schema, data);
               break;

            case TType.INT96:
               WriteInt96(writer, schema, data);
               break;

            case TType.BYTE_ARRAY:
               WriteByteArray(writer, schema, data);
               break;

            case TType.FIXED_LEN_BYTE_ARRAY:
               WriteByteArray(writer, schema, data);
               break;


            default:
               throw new NotImplementedException($"type {schema.Thrift.Type} not implemented");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteBoolean(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<bool>)data;
         int n = 0;
         byte b = 0;
         byte[] buffer = new byte[data.Count / 8 + 1];
         int ib = 0;

         foreach (bool flag in data)
         {
            if (flag)
            {
               b |= (byte)(1 << n);
            }

            if (n == 8)
            {
               buffer[ib++] = b;
               n = 0;
               b = 0;
            }

            n += 1;
         }

         if (n != 0) buffer[ib] = b;

         writer.Write(buffer);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteInt32(BinaryWriter writer, SchemaElement schema, IList data)
      {
         if (schema.IsAnnotatedWith(Thrift.ConvertedType.DATE))
         {
            var dataTyped = (List<DateTimeOffset>)data;
            foreach(DateTimeOffset el in dataTyped)
            {
               int days = (int)el.ToUnixDays();
               writer.Write(days);
            }
         }
         else
         {
            var dataTyped = (List<int>)data;
            foreach (int el in dataTyped)
            {
               writer.Write(el);
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteFloat(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<float>)data;
         foreach (float f in lst)
         {
            writer.Write(f);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteLong(BinaryWriter writer, SchemaElement schema, IList data)
      {
         if (schema.IsAnnotatedWith(Thrift.ConvertedType.TIMESTAMP_MILLIS))
         {
            var lst = (List<DateTimeOffset>)data;
            foreach(DateTimeOffset dto in lst)
            {
               long unixTime = dto.ToUnixTime();
               writer.Write(unixTime);
            }
         }
         else
         {
            var lst = (List<long>)data;
            foreach (long l in lst)
            {
               writer.Write(l);
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void WriteInt96(BinaryWriter writer, SchemaElement schema, IList data)
      {
         foreach (DateTimeOffset dto in data)
         {
            int unixTime = (int) dto.DateTime.DateTimeToJulian();
            // need to fill in the blanks here at the moment there is no day precision 
            // need to get the offset from midday from the date and add these as nanos
            // written as a long across 8 bytes
            double nanos = dto.TimeOfDay.TotalMilliseconds * 1000000D;
            writer.Write((long) nanos);
            writer.Write(unixTime);

#if DEBUG 
            // this is the writer to spit out byte arrays of what Spark would see 
            var bytes = new byte[12];
            using (var memoryStream = new MemoryStream(bytes))
            {
               using (var bWriter = new BinaryWriter(memoryStream))
               {
                  bWriter.Write(0L);
                  bWriter.Write(unixTime);
               }
            }
#endif
         }
         
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteDouble(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<double>)data;
         foreach (float d in lst)
         {
            writer.Write(d);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void WriteByteArray(BinaryWriter writer, SchemaElement schema, IList data)
      {
         if (data.Count == 0) return;

         SType elementType = data[0].GetType();
         if(elementType == typeof(string))
         {
            var src = (List<string>)data;
            foreach(string s in src)
            {
               Write(writer, s);
            }
         }
         else if(elementType == typeof(byte[]))
         {
            var src = (List<byte[]>)data;

            if (_options.TreatByteArrayAsString)
            {
               foreach(byte[] b in src)
               {
                  string s = UTF8.GetString(b);
                  Write(writer, s);
               }
            }
            else
            {
               foreach (byte[] b in src)
               {
                  writer.Write(b.Length);
                  writer.Write(b);
               }
            }
         }
         else
         {
            throw new ParquetException($"byte array type can be either byte or string but {elementType} found");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void Write(BinaryWriter writer, string s)
      {
         int length = s == null ? 0 : s.Length;
         writer.Write(length);

         if (length == 0) return;
         byte[] data = UTF8.GetBytes(s);
         writer.Write(data);
      }
   }
}
