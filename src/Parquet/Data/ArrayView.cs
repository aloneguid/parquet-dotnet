using System;
using System.Collections.Generic;

namespace Parquet.Data
{
   /// <summary>
   /// Encapsulate an array and the length of the array used 
   /// </summary>
   class ArrayView
   {
      private readonly Array _array;

      public ArrayView(Array array)
      {
         Count = array.Length;
         _array = array;
      }

      public static WritableArrayView<T> CreateWritable<T>(int length)
      {
         return new WritableArrayView<T>(length);
      }

      public virtual int Count { get; }

      protected virtual void ReturnArray()
      {
      }

      public IEnumerable<T> GetValuesAndReturnArray<T>()
      {
         T[] typed = (T[]) _array;
         for (int i = 0; i < Count; i++)
         {
            yield return typed[i];
         }

         ReturnArray();
      }

      public IEnumerable<T> GetValuesAndReturnArray<T>(DataColumnStatistics statistics, IEqualityComparer<T> equalityComparer, IComparer<T> comparer)
      {
         T[] typed = (T[]) _array;
         if (statistics == null)
         {
            for (int i = 0; i < Count; i++)
            {
               yield return typed[i];
            }

            ReturnArray();

            yield break;
         }
         // there's a big win here if you can use the ctor from NET Standard 2.1 that has a capacity, which avoids GC overhead of resize
#if NETSTANDARD2_1
         HashSet<T> hashSet = new HashSet<T>(Count, equalityComparer);
#else
         HashSet<T> hashSet = new HashSet<T>(equalityComparer);
#endif

         T min = default;
         T max = default;
         for (int i = 0; i < Count; i++)
         {
            T current = typed[i];
            yield return current;

            hashSet.Add(current);

            if (i == 0)
            {
               min = current;
               max = current;
            }
            else
            {
               int cmin = comparer.Compare(min, current);
               int cmax = comparer.Compare(max, current);

               if (cmin > 0)
               {
                  min = current;
               }

               if (cmax < 0)
               {
                  max = current;
               }
            }
         }

         statistics.MinValue = min;
         statistics.MaxValue = max;
         statistics.DistinctCount = hashSet.Count;

         ReturnArray();
      }
   }
}