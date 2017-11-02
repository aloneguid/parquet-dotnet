using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.File.Values.Primitives
{
   static class FastString
   {
      public static unsafe string Create(byte[] bytes, int index, int count)
      {
         fixed (byte* pBytes = bytes)
         {
            return Create(pBytes + index, count);
         }
      }

      private static unsafe string Create(byte* bytes, int byteLength)
      {
         throw new NotImplementedException();
         //string s = FastAllocateString(10);

         //return s;
      }

      //extern static string FastAllocateString(int length);

   }
}
