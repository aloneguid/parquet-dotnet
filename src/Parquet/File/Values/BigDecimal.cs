using System.Numerics;

namespace Parquet.File.Values
{
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