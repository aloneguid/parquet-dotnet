#define SPARK_TYPES 

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Thrift;
using TType = Parquet.Thrift.Type;
using System.Runtime.CompilerServices;
using System.Numerics;

namespace Parquet.File.Values
{
   class PlainValuesReader : IValuesReader
   {
      private static readonly System.Text.Encoding UTF8 = System.Text.Encoding.UTF8;

      public void Read(BinaryReader reader, SchemaElement schema, IList destination)
      {
         long byteCount = reader.BaseStream.Length - reader.BaseStream.Position;
         byte[] data = reader.ReadBytes((int)byteCount);

         switch (schema.Type)
         {
            case TType.BOOLEAN:
               ReadPlainBoolean(data, 8, destination);
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
            default:
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadPlainBoolean(byte[] data, int count, IList destination)
      {
         int ibit = 0;
         int ibyte = 0;
         byte b = data[0];
         var destinationTyped = (List<bool?>)destination;

         for (int ires = 0; ires < count; ires++)
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
         if(schema.Converted_type == ConvertedType.DATE)
         {
            List<DateTime?> destinationTyped = (List<DateTime?>)destination;
            for (int i = 0; i < data.Length; i += 4)
            {
               int iv = BitConverter.ToInt32(data, i);
               destinationTyped.Add(iv.FromUnixTime());
            }
         }
         else
         {
            List<int?> destinationTyped = (List<int?>)destination;
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
            destination.Add(lv);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadInt96(byte[] data, SchemaElement schema, IList destination)
      {
#if !SPARK_TYPES
         List<BigInteger?> destinationTyped = (List<BigInteger?>)destination;
#else
         List<DateTime?> destinationTyped = (List<DateTime?>)destination;
#endif

         //todo: this is a sample how to read int96, not tested this yet
         // todo: need to work this out because Spark is not encoding per spec - working with the Spark encoding instead
#if !SPARK_TYPES
         //var r96 = new List<BigInteger>(data.Length / 12);
#else
         var r96 = new List<DateTime?>(data.Length / 12);
#endif

         for (int i = 0; i < data.Length; i += 12)
         {

#if !SPARK_TYPES
            byte[] v96 = new byte[12];
            Array.Copy(data, i, v96, 0, 12);
            var bi = new BigInteger(v96);
#else
                  // for the time being we can discard the nanos 
                  var utils = new NumericUtils();
                  byte[] v96 = new byte[4];
                  byte[] nanos = new byte[8];
                  Array.Copy(data, i + 8, v96, 0, 4);
                  Array.Copy(data, i, nanos, 0, 8);
                  var bi = BitConverter.ToInt32(v96, 0).JulianToDateTime();
                  bi.AddMilliseconds((double) (BitConverter.ToInt64(nanos, 0) / 1000));
#endif
            destinationTyped.Add(bi);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadByteArray(byte[] data, SchemaElement schemaElement, IList destination)
      {
         if (schemaElement.Converted_type == ConvertedType.UTF8)
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
}
