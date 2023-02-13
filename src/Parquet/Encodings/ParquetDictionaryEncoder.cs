using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Encodings {
    static class ParquetDictionaryEncoder {

        public static bool TryExtractDictionary(Type elementType,
            Array data, int offset, int count,
            out Array? dictionaryArray,
            out int[]? rentedIndexes,
            double threshold = 0.8) {

            dictionaryArray = null;
            rentedIndexes = null;

            if(elementType != typeof(string) || count == 0)
                return false;

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
    }
}
