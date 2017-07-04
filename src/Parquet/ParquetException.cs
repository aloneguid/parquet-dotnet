using System;

namespace Parquet
{
   /// <summary>
   /// Parquet format specific
   /// </summary>
   public class ParquetException : Exception
   {
      /// <summary>
      /// Creates an instance
      /// </summary>
      public ParquetException() { }

      /// <summary>
      /// Creates an instance
      /// </summary>
      public ParquetException(string message) : base(message) { }

      /// <summary>
      /// Creates an instance
      /// </summary>
      public ParquetException(string message, Exception inner) : base(message, inner) { }
   }
}
