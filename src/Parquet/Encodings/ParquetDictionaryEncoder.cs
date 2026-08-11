using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using CommunityToolkit.HighPerformance.Buffers;

namespace Parquet.Encodings;

static class ParquetDictionaryEncoder {

    public static bool TryExtractDictionary(ReadOnlySpan<ReadOnlyMemory<char>> strings, double threshold,

        [NotNullWhen(true)]
        out IMemoryOwner<ReadOnlyMemory<char>>? dictionary,

        [NotNullWhen(true)]
        out IMemoryOwner<int>? indexes,

        int sampleSize = 0) {

        dictionary = null;
        indexes = null;

        int count = strings.Length;
        if(count == 0) {
            return false;
        }

        // Adaptive sampling: check a small sample before doing the expensive full-data scan.
        // If the sample already exceeds the threshold, skip dictionary encoding entirely.
        if(sampleSize > 0 && count > sampleSize) {
            int sampleMaxDistinct = (int)(sampleSize * threshold);
            var sampleSet = new Dictionary<ReadOnlyMemory<char>, int>(sampleSize, ReadOnlyMemoryCharOrdinalComparer.Instance);
            for(int i = 0; i < sampleSize; i++) {
                if(!sampleSet.ContainsKey(strings[i])) {
                    if(sampleSet.Count >= sampleMaxDistinct) {
                        return false;
                    }
                    sampleSet[strings[i]] = sampleSet.Count;
                }
            }
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
}