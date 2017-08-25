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

      public byte[] ToByteArray(int count)
      {
         byte[] result = new byte[count];

         var bi = new BigInteger(Value);
         byte[] data = bi.ToByteArray();
         Array.Copy(data, result, data.Length);

         result = result.Reverse().ToArray();
         return result;
      }
   }
}