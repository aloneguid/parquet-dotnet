using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace Parquet.Test
{
   public class TestBase
   {
      private static readonly string ThisPath;

      static TestBase()
      {
         ThisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
      }

      protected string GetDataFilePath(string name)
      {
         return Path.Combine(Path.GetDirectoryName(ThisPath), "data", name);
      }
   }
}