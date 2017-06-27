using System;

namespace Parquet
{
   //[System.Serializable]
   public class ParquetException : Exception
   {
      public ParquetException() { }

      public ParquetException(string message) : base(message) { }

      public ParquetException(string message, Exception inner) : base(message, inner) { }

      /*protected ParquetException(
        System.Runtime.Serialization.SerializationInfo info,
        System.Runtime.Serialization.StreamingContext context) : base(info, context) { }*/
   }
}
