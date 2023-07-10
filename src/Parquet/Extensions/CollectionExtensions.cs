using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Parquet {
    sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T> where T : class {
        public static IEqualityComparer<T> Default { get; } = new ReferenceEqualityComparer<T>();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);
        public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
    }

    static class CollectionExtensions {
        public static void TrimTail(this IList list, int maxValues) {
            if(list == null)
                return;

            if(list.Count > maxValues) {
                int diffCount = list.Count - maxValues;
                while(--diffCount >= 0)
                    list.RemoveAt(list.Count - 1); //more effective than copying the list again
            }
        }

        public static void TrimHead(this IList list, int maxValues) {
            if(list == null)
                return;

            while(list.Count > maxValues && list.Count > 0) {
                list.RemoveAt(0);
            }
        }

        /// <summary>
        /// Batch through IEnumerable without going to the beginning every time. May need optimisations but OK so far.
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int size) {
            T[]? bucket = null;
            int count = 0;

            foreach(T item in source) {
                if(bucket == null)
                    bucket = new T[size];

                bucket[count++] = item;

                if(count != size)
                    continue;

                yield return bucket.Select(x => x); 

                bucket = null;
                count = 0;
            }

            // Return the last bucket with all remaining elements
            if(bucket != null && count > 0)
                yield return bucket.Take(count);
        }

        public static IEnumerable<Tuple<TFirst, TSecond?>> IterateWith<TFirst, TSecond>(
            this IEnumerable<TFirst> firstSource, IEnumerable<TSecond> secondSource)
            where TFirst : class, new()
            where TSecond : class {
            return new DoubleIterator<TFirst, TSecond>(firstSource, secondSource);
        }

        class DoubleIterator<TFirst, TSecond> : IEnumerable<Tuple<TFirst, TSecond?>>, IEnumerator<Tuple<TFirst, TSecond?>> 
            where TFirst : class, new()
            where TSecond : class {
            private readonly IEnumerator<TFirst> _first;
            private readonly IEnumerator<TSecond>? _second;

            public DoubleIterator(IEnumerable<TFirst> first, IEnumerable<TSecond>? second) {
                _first = first.GetEnumerator();
                _second = second?.GetEnumerator();
                Current = Tuple.Create<TFirst, TSecond?>(new TFirst(), null);
            }

            public Tuple<TFirst, TSecond?> Current { get; private set; }

            object? IEnumerator.Current => Current;

            public void Dispose() {

            }

            public IEnumerator<Tuple<TFirst, TSecond?>> GetEnumerator() {
                return this;
            }

            public bool MoveNext() {
                bool canMove = _first.MoveNext() && (_second == null || _second.MoveNext());

                if(canMove) {
                    Current = new Tuple<TFirst, TSecond?>(_first.Current, _second == null ? default(TSecond) : _second.Current);
                }

                return canMove;
            }

            public void Reset() {
                _first.Reset();
                _second?.Reset();
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        public static T[] Append<T>(this T[]? array, T value) {
            if(array == null)
                return Array.Empty<T>();

            int length = array?.Length ?? 0;
            var newArray = new T[length + 1];

            if(length > 0)
                array?.CopyTo(newArray, 0);

            newArray[newArray.Length - 1] = value;

            return newArray;
        }
    }
}