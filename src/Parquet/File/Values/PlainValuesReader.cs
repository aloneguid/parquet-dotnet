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
         if (schema.IsAnnotatedWith(Thrift.ConvertedType.TIMESTAMP_MILLIS))
         {
            var lst = (List<DateTimeOffset>)destination;

            for (int i = 0; i < data.Length; i += 8)
            {
               long lv = BitConverter.ToInt64(data, i);
               lst.Add(lv.FromUnixTime());
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
         for (int i = 0; i < data.Length; i += schema.Thrift.Type_length)
         {
            if (!schema.IsAnnotatedWith(Thrift.ConvertedType.DECIMAL)) continue;
            // go from data - decimal needs to be 16 bytes but not from Spark - variable fixed nonsense
            byte[] dataNew = new byte[schema.Thrift.Type_length];
            Array.Copy(data, i, dataNew, 0, schema.Thrift.Type_length);
            var bigInt = new BigDecimal(new BigInteger(dataNew.Reverse().ToArray()), schema.Thrift.Scale, schema.Thrift.Precision);

            decimal dc = (decimal) bigInt;
            destination.Add(dc);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadInt96(byte[] data, SchemaElement schema, IList destination)
      {


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
            DateTime bi = BitConverter.ToInt32(v96, 0).JulianToDateTime();
            long nanosToInt64 = BitConverter.ToInt64(nanos, 0);
            double millis = (double) nanosToInt64 / 1000000D;
            bi = bi.AddMilliseconds(millis);
#endif
            destination.Add(new DateTimeOffset(bi));

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
