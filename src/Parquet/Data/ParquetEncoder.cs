using System;
using System.Buffers;
using System.IO;
using System.Numerics;
using System.Runtime.InteropServices;
using Parquet.File.Values.Primitives;

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
            }
            else if(t == typeof(sbyte[])) {
                Span<sbyte> span = ((sbyte[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(Int16[])) {
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
            }
            else if(t == typeof(Int64[])) {
                Span<long> span = ((long[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(BigInteger[])) {
                Span<BigInteger> span = ((BigInteger[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(decimal[])) {
                Span<decimal> span = ((decimal[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            } else if(t == typeof(double[])) {
                Span<double> span = ((double[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(float[])) {
                Span<float> span = ((float[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(byte[][])) {
                Span<byte[]> span = ((byte[][])data).AsSpan(offset, count);
                Encode(span, destination);
                return true;
            }
            else if(t == typeof(DateTime[])) {
                Span<DateTime> span = ((DateTime[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(DateTimeOffset[])) {
                Span<DateTimeOffset> span = ((DateTimeOffset[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;

            } else if(t == typeof(Interval[])) {
                Span<Interval> span = ((Interval[])data).AsSpan(offset, count);
                Encode(span, destination);
                // no stats, maybe todo
                return true;
            }

            return false;
        }

        #region [ Single Value Encoding ]

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
            else if(t == typeof(sbyte)) {
                result = BitConverter.GetBytes((int)(sbyte)value);
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
            else if(t == typeof(decimal)) {
                return TryEncode((decimal)value, tse, out result);
            } else if(t == typeof(double)) {
                result = BitConverter.GetBytes((double)value);
                return true;
            }
            else if(t == typeof(float)) {
                result = BitConverter.GetBytes((float)value);
                return true;
            }
            else if(t == typeof(byte[])) {
                result = (byte[])value;
                return true;
            }
            else if(t == typeof(DateTimeOffset)) {
                return TryEncode((DateTimeOffset)value, tse, out result);
            }
            else if(t == typeof(DateTime)) {
                return TryEncode((DateTime)value, tse, out result);
            } else if(t == typeof(Interval)) {
                result = ((Interval)value).GetBytes();
                return true;
            }

            result = null;
            return false;
        }

        private static bool TryEncode(DateTime value, Thrift.SchemaElement tse, out byte[] result) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    int days = value.ToUnixDays();
                    result = BitConverter.GetBytes(days);
                    return true;
                case Thrift.Type.INT64:
                    long unixTime = value.ToUnixMilliseconds();
                    result = BitConverter.GetBytes(unixTime);
                    return true;
                case Thrift.Type.INT96:
                    var nano = new NanoTime(value);
                    result = nano.GetBytes();
                    return true;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");

            }
        }

        private static bool TryEncode(decimal value, Thrift.SchemaElement tse, out byte[] result) {
            try {
                switch(tse.Type) {
                    case Thrift.Type.INT32:
                        double sf32 = Math.Pow(10, tse.Scale);
                        int i = (int)(value * (decimal)sf32);
                        result = BitConverter.GetBytes(i);
                        return true;
                    case Thrift.Type.INT64:
                        double sf64 = Math.Pow(10, tse.Scale);
                        long l = (long)(value * (decimal)sf64);
                        result = BitConverter.GetBytes((long)l);
                        return true;
                    case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
                        var bd = new BigDecimal(value, tse.Precision, tse.Scale);
                        result = bd.GetBytes();
                        return true;
                    default:
                        throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
                }
            }

            catch(OverflowException) {
                throw new ParquetException(
                   $"value '{value}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
            }
        }

        private static bool TryEncode(DateTimeOffset value, Thrift.SchemaElement tse, out byte[] result) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    int days = value.ToUnixDays();
                    result = BitConverter.GetBytes(days);
                    return true;
                case Thrift.Type.INT64:
                    long unixTime = value.ToUnixMilliseconds();
                    result = BitConverter.GetBytes(unixTime);
                    return true;
                case Thrift.Type.INT96:
                    var nano = new NanoTime(value);
                    result = nano.GetBytes();
                    return true;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");
            }
        }

        #endregion

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

        public static void Encode(ReadOnlySpan<sbyte> data, Stream destination) {

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
                for(int i = 0; i < data.Length; i++) {
                    ints[i] = data[i];
                }
            }
            finally {
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

        public static void Encode(ReadOnlySpan<decimal> data, Stream destination, Thrift.SchemaElement tse) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    double sf32 = Math.Pow(10, tse.Scale);
                    foreach(decimal d in data) {
                        try {
                            int i = (int)(d * (decimal)sf32);
                            byte[] b = BitConverter.GetBytes(i);
                            destination.Write(b, 0, b.Length);
                        }
                        catch(OverflowException) {
                            throw new ParquetException(
                               $"value '{d}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
                        }
                    }
                    break;
                case Thrift.Type.INT64:
                    double sf64 = Math.Pow(10, tse.Scale);
                    foreach(decimal d in data) {
                        try {
                            long l = (long)(d * (decimal)sf64);
                            byte[] b = BitConverter.GetBytes((long)l);
                            destination.Write(b, 0, b.Length);
                        }
                        catch(OverflowException) {
                            throw new ParquetException(
                               $"value '{d}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
                        }
                    }
                    break;
                case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
                    foreach(decimal d in data) {
                        var bd = new BigDecimal(d, tse.Precision, tse.Scale);
                        byte[] b = bd.GetBytes();
                        tse.Type_length = b.Length; //always re-set type length as it can differ from default type length
                        destination.Write(b, 0, b.Length);
                    }
                    break;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
            }
        }

        public static void Encode(ReadOnlySpan<double> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static void Encode(ReadOnlySpan<float> data, Stream destination) {
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

        public static void Encode(ReadOnlySpan<DateTime> data, Stream destination, Thrift.SchemaElement tse) {

            switch(tse.Type) {
                case Thrift.Type.INT32:
                    foreach(DateTime element in data) {
                        int days = element.ToUnixDays();
                        byte[] raw = BitConverter.GetBytes(days);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case Thrift.Type.INT64:
                    foreach(DateTime element in data) {
                        long unixTime = element.ToUnixMilliseconds();
                        byte[] raw = BitConverter.GetBytes(unixTime);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case Thrift.Type.INT96:
                    foreach(DateTime element in data) {
                        var nano = new NanoTime(element);
                        nano.Write(destination);
                    }
                    break;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");

            }
        }

        public static void Encode(ReadOnlySpan<DateTimeOffset> data, Stream destination, Thrift.SchemaElement tse) {

            switch(tse.Type) {
                case Thrift.Type.INT32:
                    foreach(DateTimeOffset element in data) {
                        int days = element.ToUnixDays();
                        byte[] raw = BitConverter.GetBytes(days);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case Thrift.Type.INT64:
                    foreach(DateTimeOffset element in data) {
                        long unixTime = element.ToUnixMilliseconds();
                        byte[] raw = BitConverter.GetBytes(unixTime);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case Thrift.Type.INT96:
                    foreach(DateTimeOffset element in data) {
                        var nano = new NanoTime(element);
                        nano.Write(destination);
                    }
                    break;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");

            }
        }

        public static void Encode(ReadOnlySpan<Interval> data, Stream destination) {
            foreach(Interval iv in data) {
                byte[] b = iv.GetBytes();
                destination.Write(b, 0, b.Length);
            }
        }

        #region [ .NET differences ]

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

        #endregion

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

        public static void FillStats(ReadOnlySpan<sbyte> data, DataColumnStatistics stats) {
            data.MinMax(out sbyte min, out sbyte max);
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

        public static void FillStats(ReadOnlySpan<decimal> data, DataColumnStatistics stats) {
            data.MinMax(out decimal min, out decimal max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<double> data, DataColumnStatistics stats) {
            data.MinMax(out double min, out double max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<float> data, DataColumnStatistics stats) {
            data.MinMax(out float min, out float max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<DateTime> data, DataColumnStatistics stats) {
            data.MinMax(out DateTime min, out DateTime max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<DateTimeOffset> data, DataColumnStatistics stats) {
            data.MinMax(out DateTimeOffset min, out DateTimeOffset max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        #endregion
    }
}