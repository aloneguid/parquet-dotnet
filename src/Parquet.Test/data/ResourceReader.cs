using System.IO;
using NetBox.Extensions;

namespace Parquet.Test.data
{
   static class ResourceReader
   {
      public static Stream Open(string name)
      {
         return typeof(ResourceReader).GetSameFolderEmbeddedResourceFile(name);
      }
   }
}
