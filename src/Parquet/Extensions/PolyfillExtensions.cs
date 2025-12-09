using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Parquet.Extensions; 

static class PolyfillExtensions {

#if NETSTANDARD2_0 || NETSTANDARD2_1

    #region [ Chunk ]

    private static IEnumerable<TSource[]> ArrayChunkIterator<TSource>(TSource[] source, int size) {
        int index = 0;
        while(index < source.Length) {
            TSource[] chunk = new ReadOnlySpan<TSource>(source, index, Math.Min(size, source.Length - index)).ToArray();
            index += chunk.Length;
            yield return chunk;
        }
    }

    private static IEnumerable<TSource[]> EnumerableChunkIterator<TSource>(IEnumerable<TSource> source, int size) {
        using IEnumerator<TSource> e = source.GetEnumerator();

        // Before allocating anything, make sure there's at least one element.
        if(e.MoveNext()) {
            // Now that we know we have at least one item, allocate an initial storage array. This is not
            // the array we'll yield.  It starts out small in order to avoid significantly overallocating
            // when the source has many fewer elements than the chunk size.
            int arraySize = Math.Min(size, 4);
            int i;
            do {
                var array = new TSource[arraySize];

                // Store the first item.
                array[0] = e.Current;
                i = 1;

                if(size != array.Length) {
                    // This is the first chunk. As we fill the array, grow it as needed.
                    for(; i < size && e.MoveNext(); i++) {
                        if(i >= array.Length) {
                            arraySize = (int)Math.Min((uint)size, 2 * (uint)array.Length);
                            Array.Resize(ref array, arraySize);
                        }

                        array[i] = e.Current;
                    }
                } else {
                    // For all but the first chunk, the array will already be correctly sized.
                    // We can just store into it until either it's full or MoveNext returns false.
                    TSource[] local = array; // avoid bounds checks by using cached local (`array` is lifted to iterator object as a field)
                    Debug.Assert(local.Length == size);
                    for(; (uint)i < (uint)local.Length && e.MoveNext(); i++) {
                        local[i] = e.Current;
                    }
                }

                if(i != array.Length) {
                    Array.Resize(ref array, i);
                }

                yield return array;
            }
            while(i >= size && e.MoveNext());
        }
    }

    public static IEnumerable<TSource[]> Chunk<TSource>(this IEnumerable<TSource> source, int size) {
        if(source is TSource[] array) {
            return array.Length != 0 ? ArrayChunkIterator(array, size) : [];
        }

        return EnumerableChunkIterator(source, size);
    }

    #endregion
#endif

}
