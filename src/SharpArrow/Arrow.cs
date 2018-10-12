using System;
using System.IO;

namespace SharpArrow
{
   public static class Arrow
   {
      public static ArrowFile OpenFromFile(string filePath)
      {
         if (filePath == null)
         {
            throw new ArgumentNullException(nameof(filePath));
         }

         byte[] fileData = File.ReadAllBytes(filePath);

         return new ArrowFile(fileData.AsMemory());
      }
   }
}