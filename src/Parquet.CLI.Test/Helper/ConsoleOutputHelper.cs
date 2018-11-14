using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Moq;

namespace Parquet.CLI.Test.Helper
{
   public static class ConsoleOutputHelper
   {
      public static string getConsoleOutput(IInvocationList invocations)
      {
         if (!invocations.Any())
            return string.Empty;

         return string.Concat(invocations.Where(i => i.Method.Name.Contains("Write")).Select(i => i.Arguments.OfType<string>().FirstOrDefault()));
      }
   }
}
