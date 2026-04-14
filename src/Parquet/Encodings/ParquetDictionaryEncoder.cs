using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Parquet.Encodings {
    static class ParquetDictionaryEncoder {

        public static bool TryExtractDictionary(Type elementType,
            Array data, int offset, int count,
            [NotNullWhen(true)] out Array? dictionaryArray,
            [NotNullWhen(true)] out int[]? rentedIndexes,
            double threshold = 0.8) {
            dictionaryArray = null;
            rentedIndexes = null;

            if(count == 0) {
                return false;
            }
            if(elementType == typeof(string)) {
                //Initially at least we will leave the existing string dictionary code path intact as there are some
                //string specific optimizations in place.
                return EncodeStrings(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(DateTime)) {
                return Encode<DateTime>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(decimal)) {
                return Encode<decimal>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(byte)) {
                return Encode<byte>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(short)) {
                return Encode<short>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(ushort)) {
                return Encode<ushort>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(int)) {
                return Encode<int>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(uint)) {
                return Encode<uint>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(long)) {
                return Encode<long>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(ulong)) {
                return Encode<ulong>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(float)) {
                return Encode<float>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            if(elementType == typeof(double)) {
                return Encode<double>(data, offset, count, ref dictionaryArray, ref rentedIndexes, threshold);
            }
            return false;
        }
        private static bool EncodeStrings(Array data,
            int offset,
            int count,
            [NotNullWhen(true)] ref Array? dictionaryArray,
            [NotNullWhen(true)] ref int[]? rentedIndexes,
            double threshold) {

            string[] src = (string[])data;

            // Single pass: build value-to-index map directly, eliminating the separate HashSet
            var valueToIndex = new Dictionary<string, int>(count, StringComparer.Ordinal);
            int nextIndex = 0;

            for(int i = offset; i < offset + count; i++) {
#if NET6_0_OR_GREATER
                ref int slot = ref System.Runtime.InteropServices.CollectionsMarshal
                    .GetValueRefOrAddDefault(valueToIndex, src[i], out bool existed);
                if(!existed)
                    slot = nextIndex++;
#else
                if(!valueToIndex.ContainsKey(src[i]))
                    valueToIndex[src[i]] = nextIndex++;
#endif
            }

            double factor = valueToIndex.Count / (double)count;
            if(factor > threshold)
                return false;

            // Build dictionary array ordered by assigned index
            string[] dictionary = new string[valueToIndex.Count];
            foreach(var kvp in valueToIndex)
                dictionary[kvp.Value] = kvp.Key;
            dictionaryArray = dictionary;

            // Build index array
            rentedIndexes = ArrayPool<int>.Shared.Rent(count);
            for(int isrc = offset, itgt = 0; isrc < offset + count; isrc++, itgt++)
                rentedIndexes[itgt] = valueToIndex[src[isrc]];

            return true;
        }
        private static HashSet<string> Distinct(string[] strings, int offset, int count) {

            /*
             * Use "Ordinal" comparison as it's the fastest (13 times faster than invariant).
             * .NET standard 2.0 does not have pre-allocated hash version which give a tiny performance boost.
             * Interestingly, hashcode based hash for strings is slower.
             */

#if NETSTANDARD2_0
            var hs = new HashSet<string>(StringComparer.Ordinal);
#else
            // pre-allocation is a tiny performance boost
            var hs = new HashSet<string>(strings.Length, StringComparer.Ordinal);
#endif

            for(int i = offset; i < offset + count; i++)
                hs.Add(strings[i]);

            return hs;
        }

        private static bool Encode<T>(Array data,
            int offset,
            int count,
            [NotNullWhen(true)] ref Array? dictionaryArray,
            [NotNullWhen(true)] ref int[]? rentedIndexes,
            double threshold) where T : notnull {
            var src = (T[])data;

            // Single pass: build value-to-index map directly with simpler Dictionary<T,int>
            var valueToIndex = new Dictionary<T, int>(count);
            int nextIndex = 0;

            for(int i = offset; i < offset + count; i++) {
#if NET6_0_OR_GREATER
                ref int slot = ref System.Runtime.InteropServices.CollectionsMarshal
                    .GetValueRefOrAddDefault(valueToIndex, src[i], out bool existed);
                if(!existed)
                    slot = nextIndex++;
#else
                if(!valueToIndex.ContainsKey(src[i]))
                    valueToIndex[src[i]] = nextIndex++;
#endif
            }

            double uniquenessFactor = valueToIndex.Count / (double)count;
            if(uniquenessFactor > threshold)
                return false;

            // Build dictionary array ordered by assigned index
            T[] dictionary = new T[valueToIndex.Count];
            foreach(var kvp in valueToIndex)
                dictionary[kvp.Value] = kvp.Key;
            dictionaryArray = dictionary;

            // Build index array
            rentedIndexes = ArrayPool<int>.Shared.Rent(count);
            for(int isrc = offset, itgt = 0; isrc < offset + count; isrc++, itgt++)
                rentedIndexes[itgt] = valueToIndex[src[isrc]];

            return true;
        }

        private static Dictionary<T, (int Count, int MaxRunLength, int Index)> Distinct<T>(T[] values, int offset, int count, EqualityComparer<T> equalityComparer)
           where T : notnull {
            if(values.Length == 0) {
                return new(0);
            }
            var dict = new Dictionary<T, (int Count, int MaxRunLength, int Index)>(values.Length);
            T previous = values[offset];
            int runLength = 1;
            int index = 0;
            dict[previous] = (1, 1, index++);
            for(int i = offset + 1; i < offset + count; i++) {
                T key = values[i];
                if(equalityComparer.Equals(key, previous)) {
                    (int Count, int MaxRunLength, int Index) previousData = dict[key];
                    dict[key] = (previousData.Count + 1, previousData.MaxRunLength, previousData.Index);
                } else {
                    (int Count, int MaxRunLength, int Index) previousData = dict[previous];
                    if(previousData.MaxRunLength < runLength) {
                        dict[previous] = (previousData.Count, runLength, previousData.Index);
                    }
                    if(dict.TryGetValue(key, out (int Count, int MaxRunLength, int Index) value)) {
                        dict[key] = (value.Count + 1, value.MaxRunLength, value.Index);
                    } else {
                        dict[key] = (1, 1, index++);
                    }

                    runLength = 1;
                    previous = key;
                }

            }
            return dict;
        }
    }
}
