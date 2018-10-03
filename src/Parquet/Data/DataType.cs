namespace Parquet.Data
{
   /// <summary>
   /// List of supported data types
   /// </summary>
   public enum DataType
   {
      /// <summary>
      /// Type is not specified, shouldn't be used.
      /// </summary>
      Unspecified,

      /// <summary>
      /// Boolean
      /// </summary>
      Boolean,

      /// <summary>
      /// Byte
      /// </summary>
      Byte,

      /// <summary>
      /// Signed byte data type
      /// </summary>
      SignedByte,

      /// <summary>
      /// Unsigned byte
      /// </summary>
      UnsignedByte,

      /// <summary>
      /// Short
      /// </summary>
      Short,

      /// <summary>
      /// Unsigned short
      /// </summary>
      UnsignedShort,

      /// <summary>
      /// 16 bit integer
      /// </summary>
      Int16,

      /// <summary>
      /// 16 bit unsigned integer
      /// </summary>
      UnsignedInt16,

      /// <summary>
      /// 32 bit integer
      /// </summary>
      Int32,

      /// <summary>
      /// 64 bit integer
      /// </summary>
      Int64,

      /// <summary>
      /// 96 bit integer
      /// </summary>
      Int96,

      /// <summary>
      /// Array of bytes
      /// </summary>
      ByteArray,

      /// <summary>
      /// UTF-8 string
      /// </summary>
      String,

      /// <summary>
      /// Float
      /// </summary>
      Float,

      /// <summary>
      /// Double
      /// </summary>
      Double,

      /// <summary>
      /// Decimal
      /// </summary>
      Decimal,

      /// <summary>
      /// DateTimeOffset
      /// </summary>
      DateTimeOffset,

      /// <summary>
      /// Interval
      /// </summary>
      Interval
   }
}
