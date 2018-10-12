using System;
using System.IO;

namespace SharpArrow
{
   public static class Arrow
   {
      public static void OpenFromFile(string filePath)
      {
         if (filePath == null)
         {
            throw new ArgumentNullException(nameof(filePath));
         }

         byte[] fileData = File.ReadAllBytes(filePath);

         var file = new ArrowFile(fileData.AsMemory());
      }
   }
}