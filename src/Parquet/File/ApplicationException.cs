using System;

namespace Parquet.File
{
   internal class ApplicationException : Exception
   {
      public ApplicationException()
      {
      }

      public ApplicationException(string message) : base(message)
      {
      }

      public ApplicationException(string message, Exception innerException) : base(message, innerException)
      {
      }
   }
}