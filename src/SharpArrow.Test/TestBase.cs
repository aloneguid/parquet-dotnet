using System.IO;
using System.Reflection;

namespace SharpArrow.Test
{
   public class TestBase
   {
      private static readonly string ThisPath;

      static TestBase()
      {
         ThisPath = Assembly.Load(new AssemblyName("SharpArrow.Test")).Location;
      }

      protected string GetDataFilePath(string name)
      {
         return Path.Combine(Path.GetDirectoryName(ThisPath), "TestData", name);
      }
   }
}