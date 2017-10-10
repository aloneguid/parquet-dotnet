using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using TType = Parquet.Thrift.Type;
using SType = System.Type;
using Parquet.Data;
using Parquet.File.Values.Primitives;

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

      public bool Write(BinaryWriter writer, SchemaElement schema, IList data, out IList extraValues)
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

         extraValues = null;
         return true;
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
               writer.Write(days + 1);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.DECIMAL))
         {
            var dataTyped = (List<decimal>)data;
            double scaleFactor = Math.Pow(10, schema.Thrift.Scale);
            foreach (decimal d in dataTyped)
            {
               try
               {
                  int i = (int) (d * (decimal) scaleFactor);
                  writer.Write(i);
               }
               catch (OverflowException)
               {
                  throw new ParquetException(
                     $"value '{d}' is too large to fit into scale {schema.Thrift.Scale} and precision {schema.Thrift.Precision}");
               }
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.UINT_8))
         {
            var dataTyped = (List<byte>)data;
            foreach (byte byteValue in dataTyped)
            {
               writer.Write(byteValue);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.INT_8))
         {
            var dataTyped = (List<sbyte>)data;
            foreach (sbyte byteValue in dataTyped)
            {
               writer.Write(byteValue);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.UINT_16))
         {
            var dataTyped = (List<ushort>)data;
            foreach (ushort byteValue in dataTyped)
            {
               writer.Write(byteValue);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.INT_16))
         {
            var dataTyped = (List<short>)data;
            foreach (short byteValue in dataTyped)
            {
               writer.Write(byteValue);
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
         if (schema.ElementType == typeof(DateTimeOffset))
         {
            var lst = (List<DateTimeOffset>)data;
            foreach(DateTimeOffset dto in lst)
            {
               long unixTime = dto.ToUnixTime();
               writer.Write(unixTime);
            }
         }
         else if (schema.ElementType == typeof(decimal))
         {
            var dataTyped = (List<decimal>)data;
            double scaleFactor = Math.Pow(10, schema.Thrift.Scale);

            foreach (decimal d in dataTyped)
            {
               try
               {
                  long l = (long) (d * (decimal) scaleFactor);
                  writer.Write(l);
               }
               catch (OverflowException)
               {
                  throw new ParquetException(
                     $"value '{d}' is too large to fit into scale {schema.Thrift.Scale} and precision {schema.Thrift.Precision}");
               }
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
         if(schema.ElementType == typeof(DateTimeOffset))
         {
            foreach (DateTimeOffset dto in data)
            {
               var nano = new NanoTime(dto);
               nano.Write(writer);
            }
         }
         else if (schema.ElementType == typeof(DateTime))
         {
            foreach (DateTime dtm in data)
            {
               var nano = new NanoTime(dtm.ToUniversalTime());
               nano.Write(writer);
            }
         }
         else
         {
            foreach (byte[] dto in data)
            {
               writer.Write(dto);
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteDouble(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<double>)data;
         foreach (double d in lst)
         {
            writer.Write(d);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void WriteByteArray(BinaryWriter writer, SchemaElement schema, IList data)
      {
         if (data.Count == 0) return;

         if(schema.ElementType == typeof(string))
         {
            var src = (List<string>)data;
            foreach(string s in src)
            {
               Write(writer, s);
            }
         }
         else if (schema.ElementType == typeof(Interval))
         {
            var src = (List<Interval>) data;
            foreach (Interval interval in src)
            {
               writer.Write(BitConverter.GetBytes(interval.Months));
               writer.Write(BitConverter.GetBytes(interval.Days));
               writer.Write(BitConverter.GetBytes(interval.Millis));
            }
         }
         else if (schema.ElementType == typeof(long))
         {
            var src = (List<long>)data;
            foreach (long l in src)
            {
               writer.Write(BitConverter.GetBytes(l));
            }
         }
         else if(schema.ElementType == typeof(byte[]))
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
         else if (schema.ElementType == typeof(decimal))
         {
            var src = (List<decimal>)data;
            foreach (decimal d in src)
            {
               var bd = new BigDecimal(d, schema.Thrift.Precision, schema.Thrift.Scale);
               byte[] itemData = bd.ToByteArray();

               if (!schema.Thrift.__isset.type_length)
               {
                  schema.Thrift.Type_length = itemData.Length;
               }

               writer.Write(itemData);
            }
         }
         else
         {
            throw new ParquetException($"byte array type can be either byte or string but {schema.ElementType} found");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void Write(BinaryWriter writer, string s)
      {
         if(s.Length == 0)
         {
            writer.Write((int)0);
         }
         else
         {
            //transofrm to byte array first, as we need the length of the byte buffer, not string length
            byte[] data = UTF8.GetBytes(s);
            writer.Write((int)data.Length);
            writer.Write(data);
         }
      }
   }
}
