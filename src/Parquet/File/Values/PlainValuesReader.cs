#define SPARK_TYPES 

using System;
using System.Collections;
using System.IO;
using System.Linq;
using TType = Parquet.Thrift.Type;
using System.Runtime.CompilerServices;
using System.Numerics;
using Parquet.Data;
using System.Collections.Generic;
using Parquet.File.Values.Primitives;

namespace Parquet.File.Values
{
   //see https://github.com/Parquet/parquet-format/blob/master/Encodings.md#plain-plain--0
   class PlainValuesReader : IValuesReader
   {
      private static readonly System.Text.Encoding UTF8 = System.Text.Encoding.UTF8;
      private readonly ParquetOptions _options;

      public PlainValuesReader(ParquetOptions options)
      {
         _options = options;
      }

      public void Read(BinaryReader reader, SchemaElement schema, IList destination, long maxValues)
      {
         long byteCount = reader.BaseStream.Length - reader.BaseStream.Position;
         byte[] data = reader.ReadBytes((int)byteCount);

         switch (schema.Thrift.Type)
         {
            case TType.BOOLEAN:
               ReadPlainBoolean(data, destination, maxValues);
               break;
            case TType.INT32:
               ReadInt32(data, schema, destination);
               break;
            case TType.FLOAT:
               ReadFloat(data, schema, destination);
               break;
            case TType.INT64:
               ReadLong(data, schema, destination);
               break;
            case TType.DOUBLE:
               ReadDouble(data, schema, destination);
               break;
            case TType.INT96:
               ReadInt96(data, schema, destination);
               break;
            case TType.BYTE_ARRAY:
               ReadByteArray(data, schema, destination);
               break;
            case TType.FIXED_LEN_BYTE_ARRAY:
               ReadFixedLenByteArray(data, schema, destination);
               break;
            default:
               throw new NotImplementedException($"type {schema.Thrift.Type} not implemented");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadPlainBoolean(byte[] data, IList destination, long maxValues)
      {
         int ibit = 0;
         int ibyte = 0;
         byte b = data[0];

         for(int ires = 0; ires < maxValues; ires++)
         {
            if (ibit == 8)
            {
               if (ibyte + 1 >= data.Length)
               {
                  break;
               }
               b = data[++ibyte];
               ibit = 0;
            }

            bool set = ((b >> ibit++) & 1) == 1;
            destination.Add(set);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadInt32(byte[] data, SchemaElement schema, IList destination)
      {
         if (schema.IsAnnotatedWith(Thrift.ConvertedType.DATE))
         {
            for (int i = 0; i < data.Length; i += 4)
            {
               int iv = BitConverter.ToInt32(data, i);
               destination.Add(new DateTimeOffset(iv.FromUnixTime(), TimeSpan.Zero));
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.DECIMAL))
         {
            decimal scaleFactor = (decimal) Math.Pow(10, -schema.Thrift.Scale);
            for (int i = 0; i < data.Length; i += 4)
            {
               int iv = BitConverter.ToInt32(data, i);
               decimal dv = iv * scaleFactor;
               destination.Add(dv);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.UINT_8))
         {
            foreach (byte byteValue in data)
            {
               destination.Add(byteValue);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.INT_8))
         {
            foreach (sbyte byteValue in data)
            {
               destination.Add(byteValue);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.UINT_16))
         {
            for (int i = 0; i < data.Length; i += 2)
            {
               ushort iv = BitConverter.ToUInt16(data, i);
               destination.Add(iv);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.INT_16))
         {
            for (int i = 0; i < data.Length; i += 2)
            {
               short iv = BitConverter.ToInt16(data, i);
               destination.Add(iv);
            }
         }
         else
         {
            for (int i = 0; i < data.Length; i += 4)
            {
               int iv = BitConverter.ToInt32(data, i);
               destination.Add(iv);
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadFloat(byte[] data, SchemaElement schema, IList destination)
      {
         for (int i = 0; i < data.Length; i += 4)
         {
            float iv = BitConverter.ToSingle(data, i);
            destination.Add(iv);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadLong(byte[] data, SchemaElement schema, IList destination)
      {
         if (schema.ElementType == typeof(DateTimeOffset))
         {
            var lst = (List<DateTimeOffset>)destination;

            for (int i = 0; i < data.Length; i += 8)
            {
               long lv = BitConverter.ToInt64(data, i);
               lst.Add(lv.FromUnixTime());
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.DECIMAL))
         {
            decimal scaleFactor = (decimal)Math.Pow(10, -schema.Thrift.Scale);
            for (int i = 0; i < data.Length; i += 8)
            {
               long lv = BitConverter.ToInt64(data, i);
               decimal dv = lv * scaleFactor;
               destination.Add(dv);
            }
         }
         else
         {
            for (int i = 0; i < data.Length; i += 8)
            {
               long lv = BitConverter.ToInt64(data, i);
               destination.Add(lv);
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadDouble(byte[] data, SchemaElement schema, IList destination)
      {
         for (int i = 0; i < data.Length; i += 8)
         {
            double lv = BitConverter.ToDouble(data, i);
            destination.Add(lv);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadFixedLenByteArray(byte[] data, SchemaElement schema, IList destination)
      {
         if (schema.IsAnnotatedWith(Thrift.ConvertedType.DECIMAL))
         {
            int typeLength = schema.Thrift.Type_length;
            byte[] itemData = ByteGarbage.GetByteArray(typeLength);
            for (int i = 0; i < data.Length; i += typeLength)
            {
               Array.Copy(data, i, itemData, 0, typeLength);

               decimal dc = new BigDecimal(itemData, schema.Thrift);
               destination.Add(dc);
            }
         }
         else if (schema.IsAnnotatedWith(Thrift.ConvertedType.INTERVAL))
         {
            for (int i = 0; i < data.Length; i += schema.Thrift.Type_length)
            {
               // assume this is the number of months / days / millis offset from the Julian calendar
               //todo: optimize allocations
               byte[] months = new byte[4];
               byte[] days = new byte[4];
               byte[] millis = new byte[4];
               Array.Copy(data, i, months, 0, 4);
               Array.Copy(data, i + 4, days, 0, 4);
               Array.Copy(data, i + 8, millis, 0, 4);
               destination.Add(new Interval(
                  BitConverter.ToInt32(months, 0),
                  BitConverter.ToInt32(days, 0),
                  BitConverter.ToInt32(millis, 0)));
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void ReadInt96(byte[] data, SchemaElement schema, IList destination)
      {
         for (int i = 0; i < data.Length; i += 12)
         {
            if (!_options.TreatBigIntegersAsDates)
            {
               byte[] v96 = new byte[12];
               Array.Copy(data, i, v96, 0, 12);;
               destination.Add(new BigInteger(v96));
            }
            else
            {
               var nano = new NanoTime(data, i);
               DateTimeOffset dt = nano;
               destination.Add(dt);
            }
         } 
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void ReadByteArray(byte[] data, SchemaElement schemaElement, IList destination)
      {
         // Both UTF8 and JSON are stored as binary data (byte_array) which allows annotations to be used either UTF8 and JSON 
         // They should be treated in the same way as Strings
         // need to find a better implementation for this but date strings are always broken here because of the type mismatch 
         if (schemaElement.IsAnnotatedWith(Thrift.ConvertedType.UTF8) ||
            schemaElement.IsAnnotatedWith(Thrift.ConvertedType.JSON) ||
            _options.TreatByteArrayAsString)
         {
            for (int i = 0; i < data.Length;)
            {
               int length = BitConverter.ToInt32(data, i);
               i += 4;        //fast-forward to data
               string s = UTF8.GetString(data, i, length);
               i += length;   //fast-forward to the next element
               destination.Add(s);
            }
         }
         else
         {
            for (int i = 0; i < data.Length;)
            {
               int length = BitConverter.ToInt32(data, i);
               i += 4;        //fast-forward to data
               byte[] ar = new byte[length];
               Array.Copy(data, i, ar, 0, length);
               i += length;   //fast-forward to the next element
               destination.Add(ar);
            }
         }
      }

   }
}
