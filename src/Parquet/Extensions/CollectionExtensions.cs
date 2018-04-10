using System.Collections;
using System.Collections.Generic;
using System.Linq;

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

      public static void AddOneByOne(this IList dest, IList source)
      {
         if (source == null) return;

         foreach(object o in source)
         {
            dest.Add(o);
         }
      }

      /// <summary>
      /// Batch through IEnumerable without going to the beginning every time. May need optimisations but OK so far.
      /// </summary>
      public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int size)
      {
         T[] bucket = null;
         int count = 0;

         foreach (T item in source)
         {
            if (bucket == null)
               bucket = new T[size];

            bucket[count++] = item;

            if (count != size)
               continue;

            yield return bucket.Select(x => x);

            bucket = null;
            count = 0;
         }

         // Return the last bucket with all remaining elements
         if (bucket != null && count > 0)
            yield return bucket.Take(count);
      }
   }
}
