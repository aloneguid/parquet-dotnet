using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace Parquet.Json.Test
{
   public class TestBase
   {
      protected string GetDataFilePath(string name)
      {
         string thisPath = Assembly.Load(new AssemblyName("Parquet.Json.Test")).Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }

      protected string ReadJson(string name)
      {
         return System.IO.File.ReadAllText(GetDataFilePath(name));
      }
   }
}