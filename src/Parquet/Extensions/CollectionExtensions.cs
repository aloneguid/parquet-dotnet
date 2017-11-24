using System.Collections;

namespace Parquet
{
   static class CollectionExtensions
   {
      public static void TrimTail(this IList list, int maxValues)
      {
         if (list == null) return;

         if (list.Count > maxValues)
         {
            int diffCount = list.Count - maxValues;
            while (--diffCount >= 0) list.RemoveAt(list.Count - 1); //more effective than copying the list again
         }
      }

      public static void TrimHead(this IList list, int maxValues)
      {
         if (list == null) return;

         while (list.Count > maxValues && list.Count > 0)
         {
            list.RemoveAt(0);
         }
      }

      public static void Trim(this IList list, int offset, int count)
      {
         TrimHead(list, list.Count - offset);
         TrimTail(list, count);
      }
   }
}
