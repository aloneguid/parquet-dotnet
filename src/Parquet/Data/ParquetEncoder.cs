using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Parquet.Data {

    /// <summary>
    /// Fast data encoder.
    /// Experimental.
    /// </summary>
    static class ParquetEncoder {
        public static bool Encode(Array data, int offset, int count, Stream destination) {
            Type t = data.GetType();

            if(t == typeof(int)) {
                Encode(((int[])data).AsSpan(offset, count), destination);
                return true;
            }

            return false;
        }

        public static void Encode(ReadOnlySpan<int> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        private static void Write(Stream destination, ReadOnlySpan<byte> bytes) {
#if NETSTANDARD2_0
            byte[] tmp = bytes.ToArray();
            destination.Write(tmp, 0, tmp.Length);
#else
            destination.Write(bytes);
#endif
        }

        #region [ Statistics ]

        /**
         * Statistics will make certain types of queries on Parquet files much faster.
         * The problem with statistics is they are very expensive to calculate, in particular
         * exact number of distinct values.
         * Min, Max and NullCount are relatively cheap though.
         * To calculate distincts, we used to use HashSet and it's really slow, taking more than 50% of the whole encoding process.
         * HyperLogLog is slower than HashSet though https://github.com/saguiitay/CardinalityEstimation .
         */

        public static void FillStats(ReadOnlySpan<int> data, DataColumnStatistics stats) {
            data.MinMax(out int min, out int max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        #endregion
    }
}