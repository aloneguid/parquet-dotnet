using System;
using System.Collections.Generic;

namespace Parquet.Data {
    /// <summary>
    /// Encapsulate an array and the length of the array used 
    /// </summary>
    class ArrayView {
        private readonly Array _array;
        private bool _consumed;

        public ArrayView(Array array, int offset, int count) {
            Offset = offset;
            Count = count;
            _array = array;
        }

        public static WritableArrayView<T> CreateWritable<T>(int length) {
            return new WritableArrayView<T>(length);
        }

        public virtual int Count { get; }

        public int Offset { get; }

        protected virtual void ReturnArray() {
        }

        public IEnumerable<T> GetValuesAndReturnArray<T>() {
            AssertNotConsumed();
            T[] typed = (T[])_array;
            for(int i = Offset; i < Offset + Count; i++) {
                yield return typed[i];
            }

            ReturnArray();
            _consumed = true;
        }

        public IEnumerable<T> GetValuesAndReturnArray<T>(DataColumnStatistics statistics, IEqualityComparer<T> equalityComparer, IComparer<T> comparer) {
            AssertNotConsumed();
            T[] typed = (T[])_array;
            if(statistics == null) {
                for(int i = Offset; i < Offset + Count; i++) {
                    yield return typed[i];
                }

                ReturnArray();

                yield break;
            }
            // there's a big win here if you can use the ctor from NET Standard 2.1 that has a capacity, which avoids GC overhead of resize
#if NETSTANDARD2_0
            HashSet<T> hashSet = new HashSet<T>(equalityComparer);
#else
            HashSet<T> hashSet = new HashSet<T>(Count, equalityComparer);
#endif

            T min = default;
            T max = default;
            for(int i = Offset; i < Offset + Count; i++) {
                T current = typed[i];
                yield return current;

                hashSet.Add(current);

                if(i == Offset) {
                    min = current;
                    max = current;
                }
                else {
                    int cmin = comparer.Compare(min, current);
                    int cmax = comparer.Compare(max, current);

                    if(cmin > 0) {
                        min = current;
                    }

                    if(cmax < 0) {
                        max = current;
                    }
                }
            }

            statistics.MinValue = min;
            statistics.MaxValue = max;
            statistics.DistinctCount = hashSet.Count;

            ReturnArray();
            _consumed = true;
        }

        void AssertNotConsumed() {
            if(_consumed) {
                throw new InvalidOperationException("Cannot call GetValuesAndReturnArray twice");
            }
        }
    }
}