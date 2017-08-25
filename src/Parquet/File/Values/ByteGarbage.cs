using System.Collections.Generic;

namespace Parquet.File.Values
{
   static class ByteGarbage
   {
      public static readonly byte[] EmptyByteArray = new byte[0];

      public static readonly byte[] MediumSizedByteArray = new byte[1024];

      private static Dictionary<int, byte[]> SizeToArray = new Dictionary<int, byte[]>();

      public static byte[] GetAtLeast(int count)
      {
         if (count < 1024) return MediumSizedByteArray;

         return new byte[count];
      }

      public static byte[] GetByteArray(int count)
      {
         if (!SizeToArray.TryGetValue(count, out byte[] data))
         {
            data = new byte[count];
            SizeToArray[count] = data;
         }

         return data;
      }
   }
}
