using System.Numerics;

namespace Parquet.File.Values
{
   /// <summary>
   /// A class that encapsulates BigDecimal like the java class
   /// </summary>
   struct BigDecimal
   {
      /// <summary>
      /// Contains a Decimal value that is the big integer
      /// </summary>
      public decimal Integer { get; set; }
      /// <summary>
      /// The scale of the decimal value
      /// </summary>
      public int Scale { get; set; }
      /// <summary>
      /// The precision of the decimal value
      /// </summary>
      public int Precision { get; set; }
      /// <summary>
      /// Used to construct a BigDecimal
      /// </summary>
      /// <param name="integer">The <c ref="BigInteger"></c> value</param>
      /// <param name="scale">The scale of the decimal</param>
      /// <param name="precision">The precision of the decimal</param>
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
      /// <summary>
      /// Converts a BigDecimal to a decimal
      /// </summary>
      /// <param name="bd">The BigDecimal value</param>
      public static explicit operator decimal(BigDecimal bd)
      {
         return bd.Integer;
      }

      // TODO: Add to byte array for writer
   }
}