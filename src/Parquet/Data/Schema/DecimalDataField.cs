using System;

namespace Parquet.Data
{
   /// <summary>
   /// Maps to Parquet decimal type, allowing to specify custom scale and precision
   /// </summary>
   public class DecimalDataField : DataField
   {
      /// <summary>
      /// Precision
      /// </summary>
      public int Precision { get; }

      /// <summary>
      /// Scale
      /// </summary>
      public int Scale { get; }

      /// <summary>
      /// Gets a flag indicating whether byte array encoding is forced.
      /// </summary>
      public bool ForceByteArrayEncoding { get; }

      /// <summary>
      /// Constructs class instance
      /// </summary>
      /// <param name="name">The name of the column</param>
      /// <param name="precision">Cusom precision</param>
      /// <param name="scale">Custom scale</param>
      /// <param name="forceByteArrayEncoding">Whether to force decimal type encoding as fixed bytes. Hive and Impala only understands decimals when forced to true.</param>
      /// <param name="hasNulls">Is 'decimal?'</param>
      /// <param name="isArray">Indicates whether this field is repeatable.</param>
      public DecimalDataField(string name, int precision, int scale, bool forceByteArrayEncoding = false, bool hasNulls = true, bool isArray = false)
         : base(name, DataType.Decimal, hasNulls, isArray)
      {
         if (precision < 1) throw new ArgumentException("precision cannot be less than 1", nameof(precision));
         if (scale < 1) throw new ArgumentException("scale cannot be less than 1", nameof(scale));

         Precision = precision;
         Scale = scale;
         ForceByteArrayEncoding = forceByteArrayEncoding;
      }
   }
}