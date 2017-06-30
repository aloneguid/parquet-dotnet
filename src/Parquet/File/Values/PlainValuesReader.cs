#define SPARK_TYPES 

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Thrift;
using TType = Parquet.Thrift.Type;
using System.Runtime.CompilerServices;
using System.Numerics;

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

         switch (schema.Type)
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
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadPlainBoolean(byte[] data, IList destination, long maxValues)
      {
         int ibit = 0;
         int ibyte = 0;
         byte b = data[0];
         var destinationTyped = (List<bool?>)destination;

         for(int ires = 0; ires < maxValues; ires++)
         {
            if (ibit == 8)
            {
               b = data[++ibyte];
               ibit = 0;
            }

            bool set = ((b >> ibit++) & 1) == 1;
            destinationTyped.Add(set);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadInt32(byte[] data, SchemaElement schema, IList destination)
      {
         if(schema.Converted_type == ConvertedType.DATE)
         {
            List<DateTimeOffset?> destinationTyped = (List<DateTimeOffset?>)destination;
            for (int i = 0; i < data.Length; i += 4)
            {
               int iv = BitConverter.ToInt32(data, i);
               destinationTyped.Add(new DateTimeOffset(iv.FromUnixTime(), TimeSpan.Zero));
            }
         }
         else
         {
            List<int?> destinationTyped = (List<int?>)destination;
            for (int i = 0; i < data.Length; i += 4)
            {
               int iv = BitConverter.ToInt32(data, i);
               destinationTyped.Add(iv);
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadFloat(byte[] data, SchemaElement schema, IList destination)
      {
         List<float?> destinationTyped = (List<float?>)destination;
         for (int i = 0; i < data.Length; i += 4)
         {
            float iv = BitConverter.ToSingle(data, i);
            destinationTyped.Add(iv);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadLong(byte[] data, SchemaElement schema, IList destination)
      {
         List<long?> destinationTyped = (List<long?>)destination;
         for (int i = 0; i < data.Length; i += 8)
         {
            long lv = BitConverter.ToInt64(data, i);
            destinationTyped.Add(lv);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadDouble(byte[] data, SchemaElement schema, IList destination)
      {
         List<double?> destinationTyped = (List<double?>)destination;
         for (int i = 0; i < data.Length; i += 8)
         {
            double lv = BitConverter.ToDouble(data, i);
            destinationTyped.Add(lv);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadFixedLenByteArray(byte[] data, SchemaElement schema, IList destination)
      {
         List<decimal?> destinationTyped = (List<decimal?>) destination;
         for (int i = 0; i < data.Length; i += schema.Type_length)
         {
            if (schema.Converted_type != ConvertedType.DECIMAL) continue;
            // go from data - decimal needs to be 16 bytes but not from Spark - variable fixed nonsense
            byte[] dataNew = new byte[schema.Type_length];
            Array.Copy(data, i, dataNew, 0, schema.Type_length);
            var bigInt = new BigDecimal(new BigInteger(dataNew.Reverse().ToArray()), schema.Scale, schema.Precision);

            decimal dc = (decimal) bigInt;
            destinationTyped.Add(dc);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadInt96(byte[] data, SchemaElement schema, IList destination)
      {
#if !SPARK_TYPES
         List<BigInteger?> destinationTyped = (List<BigInteger?>)destination;
#else
         List<DateTimeOffset?> destinationTyped = (List<DateTimeOffset?>)destination;
#endif

         //todo: this is a sample how to read int96, not tested this yet
         // todo: need to work this out because Spark is not encoding per spec - working with the Spark encoding instead
#if !SPARK_TYPES
         //var r96 = new List<BigInteger>(data.Length / 12);
#else
         //var r96 = new List<DateTimeOffset?>(data.Length / 12);
#endif

         for (int i = 0; i < data.Length; i += 12)
         {

#if !SPARK_TYPES
            byte[] v96 = new byte[12];
            Array.Copy(data, i, v96, 0, 12);
            var bi = new BigInteger(v96);
#else
            // for the time being we can discard the nanos 
            byte[] v96 = new byte[4];
            byte[] nanos = new byte[8];
            Array.Copy(data, i + 8, v96, 0, 4);
            Array.Copy(data, i, nanos, 0, 8);
            var bi = BitConverter.ToInt32(v96, 0).JulianToDateTime();
            long nanosToInt64 = BitConverter.ToInt64(nanos, 0);
            double millis = (double) nanosToInt64 / 1000000D;
            bi = bi.AddMilliseconds(millis);
#endif
            destinationTyped.Add(new DateTimeOffset(bi));
         } 
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void ReadByteArray(byte[] data, SchemaElement schemaElement, IList destination)
      {
         // Both UTF8 and JSON are stored as binary data (byte_array) which allows annotations to be used either UTF8 and JSON 
         // They should be treated in the same way as Strings
         if (
               (schemaElement.__isset.converted_type && (schemaElement.Converted_type == ConvertedType.UTF8 || schemaElement.Converted_type == ConvertedType.JSON) ||
               (_options.TreatByteArrayAsString)
            ))
         {
            List<string> destinationTyped = (List<string>)destination;
            for (int i = 0; i < data.Length;)
            {
               int length = BitConverter.ToInt32(data, i);
               i += 4;        //fast-forward to data
               string s = UTF8.GetString(data, i, length);
               i += length;   //fast-forward to the next element
               destinationTyped.Add(s);
            }
         }
         else
         {
            List<byte[]> destinationTyped = (List<byte[]>)destination;
            for (int i = 0; i < data.Length;)
            {
               int length = BitConverter.ToInt32(data, i);
               i += 4;        //fast-forward to data
               byte[] ar = new byte[length];
               Array.Copy(data, i, ar, 0, length);
               i += length;   //fast-forward to the next element
               destinationTyped.Add(ar);
            }
         }
      }

   }


   public struct BigDecimal
   {
      public decimal Integer { get; set; }
      public int Scale { get; set; }
      public int Precision { get; set; }

      public BigDecimal(BigInteger integer, int scale, int precision) : this()
      {
         Integer = (decimal) integer;
         Scale = scale;
         Precision = precision;
         while (Scale > 0)
         {
            Integer /= 10;
            Scale -= 1;
         }
         Scale = scale;
      }

      public static explicit operator decimal(BigDecimal bd)
      {
         return bd.Integer;
      }

      // TODO: Add to byte array for writer




   }
}
