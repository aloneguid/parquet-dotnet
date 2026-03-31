using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using CommunityToolkit.HighPerformance.Buffers;

namespace Parquet.Encodings;

static class ParquetDictionaryEncoder {

    public static bool TryExtractDictionary<T>(ReadOnlySpan<T> values, double threshold,

        [NotNullWhen(true)]
        out IMemoryOwner<T>? dictionary,

        [NotNullWhen(true)]
        out IMemoryOwner<int>? indexes) where T : struct {

        dictionary = null;
        indexes = null;

        int count = values.Length;
        if(count == 0) {
            return false;
        }

        // Calculate max allowed distinct values based on threshold
        int maxDistinct = (int)(count * threshold);

        // Dictionary to track unique values and their indices
        var valueToIndex = new Dictionary<T, int>(count);

        // Rent memory for indexes
        var indexesOwner = MemoryOwner<int>.Allocate(count);
        Span<int> indexesSpan = indexesOwner.Span;

        // Single pass: build dictionary and indexes simultaneously, exit early if threshold exceeded
        for(int i = 0; i < count; i++) {
            T value = values[i];

            if(!valueToIndex.TryGetValue(value, out int index)) {
                // New unique value - check threshold before adding
                if(valueToIndex.Count >= maxDistinct) {
                    indexesOwner.Dispose();
                    return false;
                }

                index = valueToIndex.Count;
                valueToIndex[value] = index;
            }

            indexesSpan[i] = index;
        }

        // Build dictionary array from unique values
        var dictionaryOwner = MemoryOwner<T>.Allocate(valueToIndex.Count);
        Span<T> dictionarySpan = dictionaryOwner.Span;

        foreach(KeyValuePair<T, int> kvp in valueToIndex) {
            dictionarySpan[kvp.Value] = kvp.Key;
        }

        dictionary = dictionaryOwner;
        indexes = indexesOwner;
        return true;
    }

    public static bool TryExtractDictionary(ReadOnlySpan<ReadOnlyMemory<char>> strings, double threshold,

        [NotNullWhen(true)]
        out IMemoryOwner<ReadOnlyMemory<char>>? dictionary,

        [NotNullWhen(true)]
        out IMemoryOwner<int>? indexes) {

        dictionary = null;
        indexes = null;

        int count = strings.Length;
        if(count == 0) {
            return false;
        }

        // Calculate max allowed distinct values based on threshold
        int maxDistinct = (int)(count * threshold);

        // Dictionary to track unique values and their indices
        var valueToIndex = new Dictionary<ReadOnlyMemory<char>, int>(count, ReadOnlyMemoryCharOrdinalComparer.Instance);

        // Rent memory for indexes
        var indexesOwner = MemoryOwner<int>.Allocate(count);
        Span<int> indexesSpan = indexesOwner.Span;

        // Single pass: build dictionary and indexes simultaneously, exit early if threshold exceeded
        for(int i = 0; i < count; i++) {
            ReadOnlyMemory<char> value = strings[i];

            if(!valueToIndex.TryGetValue(value, out int index)) {
                // New unique value - check threshold before adding
                if(valueToIndex.Count >= maxDistinct) {
                    indexesOwner.Dispose();
                    return false;
                }

                index = valueToIndex.Count;
                valueToIndex[value] = index;
            }

            indexesSpan[i] = index;
        }

        // Build dictionary array from unique values
        var dictionaryOwner = MemoryOwner<ReadOnlyMemory<char>>.Allocate(valueToIndex.Count);
        Span<ReadOnlyMemory<char>> dictionarySpan = dictionaryOwner.Span;

        foreach(KeyValuePair<ReadOnlyMemory<char>, int> kvp in valueToIndex) {
            dictionarySpan[kvp.Value] = kvp.Key;
        }

        dictionary = dictionaryOwner;
        indexes = indexesOwner;
        return true;
    }

    private sealed class ReadOnlyMemoryCharOrdinalComparer : IEqualityComparer<ReadOnlyMemory<char>> {
        public static readonly ReadOnlyMemoryCharOrdinalComparer Instance = new ReadOnlyMemoryCharOrdinalComparer();

        public bool Equals(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) {
            return x.Span.SequenceEqual(y.Span);
        }

        public int GetHashCode(ReadOnlyMemory<char> obj) {
            return string.GetHashCode(obj.Span, StringComparison.Ordinal);
        }
    }

    // --- all legacy below ---

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
        HashSet<string> distinctSet = Distinct(src, offset, count);
        double factor = distinctSet.Count / (double)count;
        if(factor > threshold)
            return false;

        // extract indexes
        string[] dictionary = distinctSet.ToArray();
        dictionaryArray = dictionary;
        var valueToIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        for(int i = 0; i < dictionary.Length; i++)
            valueToIndex[dictionary[i]] = i;

        rentedIndexes = ArrayPool<int>.Shared.Rent(count);
        for(int isrc = offset, itgt = 0; isrc < offset + count; isrc++, itgt++)
            rentedIndexes[itgt] = valueToIndex[src[isrc]];

        return true;
    }
    private static HashSet<string> Distinct(string[] strings, int offset, int count) {

        /*
         * Use "Ordinal" comparison as it's the fastest (13 times faster than invariant).
         * Interestingly, hashcode based hash for strings is slower.
         */

        // pre-allocation is a tiny performance boost
        var hs = new HashSet<string>(strings.Length, StringComparer.Ordinal);

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

        //TODO: calculate some more statistics beyond uniquness like run lengths, index size and index bitwidth to determine if there is value
        //in dictionary encoding this data vs PLAIN encoding
        //e.g. Dictionary encoding for byte values could be worse than plain even with 50% uniqueness depending on run lengths and value spread
        Dictionary<T, (int Count, int MaxRunLength, int Index)> distinctSet = Distinct(src, offset, count, EqualityComparer<T>.Default);
        double uniquenessFactor = distinctSet.Count / (double)count;
        if(uniquenessFactor > threshold)
            return false;

        T[] dictionary = distinctSet.Keys.ToArray();
        dictionaryArray = dictionary;

        rentedIndexes = ArrayPool<int>.Shared.Rent(count);
        for(int isrc = offset, itgt = 0; isrc < offset + count; isrc++, itgt++)
            rentedIndexes[itgt] = distinctSet[src[isrc]].Index;

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