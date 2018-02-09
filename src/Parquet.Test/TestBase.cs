using System;
using System.IO;
using System.Reflection;
using Parquet.Test.data;

namespace Parquet.Test
{
   public class TestBase
   {
      protected Stream OpenTestFile(string name)
      {
         return ResourceReader.Open(name);
      }
   }
}