using System;
using System.Linq;
using System.Numerics;

namespace Parquet.File.Values.Primitives
{
   /// <summary>
   /// A class that encapsulates BigDecimal like the java class
   /// </summary>
   struct BigDecimal
   {
      /// <summary>
      /// Contains a Decimal value that is the big integer
      /// </summary>
      public decimal OriginalValue { get; set; }

      public decimal Value { get; set; }

      /// <summary>
      /// The scale of the decimal value
      /// </summary>
      public int Scale { get; set; }

      /// <summary>
      /// The precision of the decimal value
      /// </summary>
      public int Precision { get; set; }

      public BigDecimal(byte[] data, Thrift.SchemaElement schema)
      {
         data = data.Reverse().ToArray();
         OriginalValue = (decimal)(new BigInteger(data));
         Scale = schema.Scale;
         Precision = schema.Precision;

         decimal itv = OriginalValue;
         int itsc = Scale;
         while (itsc > 0)
         {
            itv /= 10;
            itsc -= 1;
         }

         Value = itv;
      }

      public BigDecimal(decimal d)
      {
         uint[] bits = (uint[]) (object) decimal.GetBits(d);

         decimal mantissa =
            (bits[2] * 4294967296m * 4294967296m) +
            (bits[1] * 4294967296m) +
            bits[0];

         uint scale = (bits[3] >> 16) & 31;

         uint precision = 0;
         if (d != 0m)
         {
            for (decimal tmp = mantissa; tmp >= 1; tmp /= 10)
            {
               precision++;
            }
         }
         else
         {
            // Handle zero differently. It's odd.
            precision = scale + 1;
         }

         Scale = (int)scale;
         Precision = (int)precision;
         OriginalValue = d;

         Value = d;
         int itsc = Scale;
         while (itsc-- > 0)
         {
            Value *= 10m;
         }

      }

      public BigDecimal(decimal d, int precision, int scale)
      {
         OriginalValue = d;
         Precision = precision;
         Scale = scale;

         decimal value = d;
         while (scale-- > 0) value *= 10m;
         Value = value;
      }

      /// <summary>
      /// Converts a BigDecimal to a decimal
      /// </summary>
      /// <param name="bd">The BigDecimal value</param>
      public static implicit operator decimal(BigDecimal bd)
      {
         return bd.Value;
      }

      private byte[] AllocateResult()
      {
         int size;

         //according to impala source: http://impala.io/doc/html/parquet-common_8h_source.html

         switch (Precision)
         {
            case 1: case 2:
               size = 1;
               break;
            case 3: case 4:
               size = 2;
               break;
            case 5: case 6:
               size = 3;
               break;
            case 7: case 8: case 9:
               size = 4;
               break;
            case 10: case 11:
               size = 5;
               break;
            case 12: case 13: case 14:
               size = 6;
               break;
            case 15: case 16:
               size = 7;
               break;
            case 17: case 18:
               size = 8;
               break;
            case 19: case 20: case 21:
               size = 9;
               break;
            case 22: case 23:
               size = 10;
               break;
            case 24: case 25: case 26:
               size = 11;
               break;
            case 27: case 28:
               size = 12;
               break;
            case 29: case 30: case 31:
               size = 13;
               break;
            case 32: case 33:
               size = 14;
               break;
            case 34: case 35:
               size = 15;
               break;
            case 36: case 37: case 38:
               size = 16;
               break;
            default:
               size = 16;
               break;
         }

         return new byte[size];
      }

      public byte[] ToByteArray()
      {
         byte[] result = AllocateResult();

         var bi = new BigInteger(Value);
         byte[] data = bi.ToByteArray();

         if (data.Length > result.Length) throw new NotSupportedException($"decimal data buffer is {data.Length} but result must fit into {result.Length} bytes");

         Array.Copy(data, result, data.Length);

         result = result.Reverse().ToArray();
         return result;
      }
   }
}