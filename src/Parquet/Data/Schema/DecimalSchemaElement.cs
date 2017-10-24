using System;
using Parquet.File.Values.Primitives;

namespace Parquet.Data
{
   /// <summary>
   /// Maps to Parquet decimal type, allowing to specify custom scale and precision
   /// </summary>
   public class DecimalSchemaElement : SchemaElement
   {
      /// <summary>
      /// Constructs class instance
      /// </summary>
      /// <param name="name">The name of the column</param>
      /// <param name="precision">Cusom precision</param>
      /// <param name="scale">Custom scale</param>
      /// <param name="forceByteArrayEncoding">Whether to force decimal type encoding as fixed bytes. Hive and Impala only understands decimals when forced to true.</param>
      /// <param name="nullable">Is 'decimal?'</param>
      public DecimalSchemaElement(string name, int precision, int scale, bool forceByteArrayEncoding = false, bool nullable = false) : base(name, nullable)
      {
         if (precision < 1) throw new ArgumentException("precision cannot be less than 1", nameof(precision));
         if (scale < 1) throw new ArgumentException("scale cannot be less than 1", nameof(scale));

         Thrift.Type tt;

         if (forceByteArrayEncoding)
         {
            tt = Parquet.Thrift.Type.FIXED_LEN_BYTE_ARRAY;
         }
         else
         {
            if (precision <= 9)
               tt = Parquet.Thrift.Type.INT32;
            else if (precision <= 18)
               tt = Parquet.Thrift.Type.INT64;
            else
               tt = Parquet.Thrift.Type.FIXED_LEN_BYTE_ARRAY;
         }

         Thrift.Type = tt;
         Thrift.Converted_type = Parquet.Thrift.ConvertedType.DECIMAL;
         Thrift.Precision = precision;
         Thrift.Scale = scale;
         Thrift.Type_length = BigDecimal.GetBufferSize(precision);
         ElementType = ColumnType = typeof(decimal);
      }
   }
}