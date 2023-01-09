using System;
using System.Buffers;
using System.IO;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Parquet.Data {

    /// <summary>
    /// Fast data encoder.
    /// Experimental.
    /// </summary>
    static class ParquetEncoder {
        public static bool Encode(
            Array data, int offset, int count,
            Thrift.SchemaElement tse,
            Stream destination,
            DataColumnStatistics stats = null) {
            Type t = data.GetType();

            if(t == typeof(bool[])) {
                Span<bool> span = ((bool[])data).AsSpan(offset, count);
                Encode(span, destination);
                // no stats for bools
                return true;
            }
            else if(t == typeof(byte[])) {
                Span<byte> span = ((byte[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            } else if(t == typeof(Int16[])) {
                Span<short> span = ((short[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(Int32[])) {
                Span<int> span = ((int[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            } else if(t == typeof(Int64[])) {
                Span<long> span = ((long[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            } else if(t == typeof(BigInteger[])) {
                Span<BigInteger> span = ((BigInteger[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            } else if(t == typeof(byte[][])) {
                Span<byte[]> span = ((byte[][])data).AsSpan(offset, count);
                Encode(span, destination);
                return true;
            }

            return false;
        }

        public static bool TryEncode(object value, Thrift.SchemaElement tse, out byte[] result) {
            if(value == null) {
                result = null;
                return false;
            }

            Type t = value.GetType();
            if(t == typeof(bool)) {
                result = null;
                return true;
            }
            else if(t == typeof(byte)) {
                result = BitConverter.GetBytes((int)(byte)value);
                return true;
            }
            else if(t == typeof(Int16)) {
                result = BitConverter.GetBytes((int)(short)value);
                return true;
            }
            else if(t == typeof(Int32)) {
                result = BitConverter.GetBytes((int)value);
                return true;
            }
            else if(t == typeof(Int64)) {
                result = BitConverter.GetBytes((long)value);
                return true;
            }
            else if(t == typeof(BigInteger)) {
                result = ((BigInteger)value).ToByteArray();
                return true;
            }
            else if(t == typeof(byte[])) {
                result = (byte[])value;
                return true;
            }

            result = null;
            return false;
        }

        public static void Encode(ReadOnlySpan<bool> data, Stream destination) {
            int targetLength = (data.Length / 8) + 1;
            byte[] buffer = ArrayPool<byte>.Shared.Rent(targetLength);

            int n = 0;
            byte b = 0;
            int ib = 0;

            try {
                foreach(bool flag in data) {
                    if(flag) {
                        b |= (byte)(1 << n);
                    }

                    n++;
                    if(n == 8) {
                        buffer[ib++] = b;
                        n = 0;
                        b = 0;
                    }
                }

                if(n != 0)
                    buffer[ib] = b;

                Write(destination, buffer.AsSpan(0, targetLength));

            }
            finally {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        public static void Encode(ReadOnlySpan<byte> data, Stream destination) {

            // copy shorts into ints
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                for(int i = 0; i < data.Length; i++) {
                    ints[i] = data[i];
                }
            }
            finally {
                ArrayPool<int>.Shared.Return(ints);
            }

            Encode(ints.AsSpan(0, data.Length), destination);
        }

        public static void Encode(ReadOnlySpan<short> data, Stream destination) {

            // copy shorts into ints
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                for(int i = 0; i < data.Length ; i++) {
                    ints[i] = data[i];
                }
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }

            Encode(ints.AsSpan(0, data.Length), destination);
        }

        public static void Encode(ReadOnlySpan<int> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode(Stream source, Span<int> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes);
        }

        public static void Encode(ReadOnlySpan<long> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static void Encode(ReadOnlySpan<BigInteger> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static void Encode(ReadOnlySpan<byte[]> data, Stream destination) {
            foreach(byte[] element in data) {
                byte[] l = BitConverter.GetBytes(element.Length);
                destination.Write(l, 0, l.Length);
                destination.Write(element, 0, element.Length);
            }
        }

        private static void Write(Stream destination, ReadOnlySpan<byte> bytes) {
#if NETSTANDARD2_0
            byte[] tmp = bytes.ToArray();
            destination.Write(tmp, 0, tmp.Length);
#else
            destination.Write(bytes);
#endif
        }

        private static int Read(Stream source, Span<byte> bytes) {
#if NETSTANDARD2_0
            byte[] tmp = new byte[bytes.Length];
            int read = 0;
            while(read < tmp.Length) {
                int r0 = source.Read(tmp, read, tmp.Length);
                if(r0 == 0)
                    break;
                read += r0;
            }
            tmp.CopyTo(bytes);
            return read;
#else
            int read = 0;
            while(read < bytes.Length) {
                int r0 = source.Read(bytes.Slice(read));
                if(r0 == 0)
                    break;
                read += r0;
            }
            return read;
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

        public static void FillStats(ReadOnlySpan<byte> data, DataColumnStatistics stats) {
            data.MinMax(out byte min, out byte max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<short> data, DataColumnStatistics stats) {
            data.MinMax(out short min, out short max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<int> data, DataColumnStatistics stats) {
            data.MinMax(out int min, out int max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<long> data, DataColumnStatistics stats) {
            data.MinMax(out long min, out long max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<BigInteger> data, DataColumnStatistics stats) {
            data.MinMax(out BigInteger min, out BigInteger max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        #endregion
    }
}