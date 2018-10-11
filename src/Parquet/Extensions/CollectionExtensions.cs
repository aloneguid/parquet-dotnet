using System;
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

      public static IEnumerable<Tuple<TFirst, TSecond>> IterateWith<TFirst, TSecond>(this IEnumerable<TFirst> firstSource, IEnumerable<TSecond> secondSource)
      {
         return new DoubleIterator<TFirst, TSecond>(firstSource, secondSource);
      }

      class DoubleIterator<TFirst, TSecond> : IEnumerable<Tuple<TFirst, TSecond>>, IEnumerator<Tuple<TFirst, TSecond>>
      {
         private readonly IEnumerator<TFirst> _first;
         private readonly IEnumerator<TSecond> _second;

         public DoubleIterator(IEnumerable<TFirst> first, IEnumerable<TSecond> second)
         {
            if (first == null)
            {
               throw new ArgumentNullException(nameof(first));
            }

            _first = first.GetEnumerator();
            _second = second?.GetEnumerator();
         }

         public Tuple<TFirst, TSecond> Current { get; private set; }

         object IEnumerator.Current => Current;

         public void Dispose()
         {

         }

         public IEnumerator<Tuple<TFirst, TSecond>> GetEnumerator()
         {
            return this;
         }

         public bool MoveNext()
         {
            bool canMove = _first.MoveNext() && (_second == null || _second.MoveNext());

            if(canMove)
            {
               Current = new Tuple<TFirst, TSecond>(_first.Current, _second == null ? default(TSecond) : _second.Current);
            }
            else
            {
               Current = null;
            }

            return canMove;
         }

         public void Reset()
         {
            _first.Reset();
            _second?.Reset();
         }

         IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
      }
   }
}
