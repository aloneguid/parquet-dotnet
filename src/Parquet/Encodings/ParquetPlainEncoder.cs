using System;
using System.Buffers;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.File.Values.Primitives;
using Parquet.Meta;
using TType = Parquet.Meta.Type;

namespace Parquet.Encodings {

    /// <summary>
    /// Fast data encoder.
    /// Experimental.
    /// </summary>
    static class ParquetPlainEncoder {

        private static readonly System.Text.Encoding E = System.Text.Encoding.UTF8;
        private static readonly byte[] ZeroInt32 = BitConverter.GetBytes(0);
        private static readonly ArrayPool<byte> BytePool = ArrayPool<byte>.Shared;

        public static void Encode(
            Array data, int offset, int count,
            SchemaElement tse,
            Stream destination,
            DataColumnStatistics? stats = null) {
            System.Type t = data.GetType();

            if(t == typeof(bool[])) {
                Span<bool> span = ((bool[])data).AsSpan(offset, count);
                Encode(span, destination);
                // no stats for bools
            } else if(t == typeof(byte[])) {
                Span<byte> span = ((byte[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(sbyte[])) {
                Span<sbyte> span = ((sbyte[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(short[])) {
                Span<short> span = ((short[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(ushort[])) {
                Span<ushort> span = ((ushort[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(int[])) {
                Span<int> span = ((int[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(uint[])) {
                Span<uint> span = ((uint[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(long[])) {
                Span<long> span = ((long[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(ulong[])) {
                Span<ulong> span = ((ulong[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(BigInteger[])) {
                Span<BigInteger> span = ((BigInteger[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(decimal[])) {
                Span<decimal> span = ((decimal[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(double[])) {
                Span<double> span = ((double[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(float[])) {
                Span<float> span = ((float[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(byte[][])) {
                Span<byte[]> span = ((byte[][])data).AsSpan(offset, count);
                Encode(span, destination);
            } else if(t == typeof(DateTime[])) {
                Span<DateTime> span = ((DateTime[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null)
                    FillStats(span, stats);
#if NET6_0_OR_GREATER
            } else if(t == typeof(DateOnly[])) {
                Span<DateOnly> span = ((DateOnly[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(TimeOnly[])) {
                Span<TimeOnly> span = ((TimeOnly[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null)
                    FillStats(span, stats);
#endif
            } else if(t == typeof(TimeSpan[])) {
                Span<TimeSpan> span = ((TimeSpan[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(Interval[])) {
                Span<Interval> span = ((Interval[])data).AsSpan(offset, count);
                Encode(span, destination);
                // no stats, maybe todo
            } else if(t == typeof(string[])) {
                Span<string> span = ((string[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null)
                    FillStats(span, stats);
            } else if(t == typeof(Guid[])) {
                Span<Guid> span = ((Guid[])data).AsSpan(offset, count);
                Encode(span, destination);
            } else {
                throw new NotSupportedException($"no PLAIN encoder exists for {t}");
            }
        }

        public static void Decode(
            Array data, int offset, int count,
            SchemaElement tse,
            Span<byte> source,
            out int elementsRead) {

            int rem = data.Length - offset;
            if(count > rem)
                count = rem;

            System.Type t = data.GetType();

            if(t == typeof(bool[])) {
                elementsRead = Decode(source, ((bool[])data).AsSpan(offset, count));
            } else if(t == typeof(byte[])) {
                elementsRead = Decode(source, ((byte[])data).AsSpan(offset, count));
            } else if(t == typeof(sbyte[])) {
                elementsRead = Decode(source, ((sbyte[])data).AsSpan(offset, count));
            } else if(t == typeof(short[])) {
                elementsRead = Decode(source, ((short[])data).AsSpan(offset, count));
            } else if(t == typeof(ushort[])) {
                elementsRead = Decode(source, ((ushort[])data).AsSpan(offset, count));
            } else if(t == typeof(int[])) {
                Span<int> span = ((int[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(uint[])) {
                Span<uint> span = ((uint[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(long[])) {
                Span<long> span = ((long[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(ulong[])) {
                Span<ulong> span = ((ulong[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(BigInteger[])) {
                Span<BigInteger> span = ((BigInteger[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(decimal[])) {
                Span<decimal> span = ((decimal[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
            } else if(t == typeof(double[])) {
                Span<double> span = ((double[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(float[])) {
                Span<float> span = ((float[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(byte[][])) {
                Span<byte[]> span = ((byte[][])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
            } else if(t == typeof(DateTime[])) {
                Span<DateTime> span = ((DateTime[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
#if NET6_0_OR_GREATER
            } else if(t == typeof(DateOnly[])) {
                Span<DateOnly> span = ((DateOnly[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
            } else if(t == typeof(TimeOnly[])) {
                Span<TimeOnly> span = ((TimeOnly[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
#endif
            } else if(t == typeof(TimeSpan[])) {
                Span<TimeSpan> span = ((TimeSpan[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
            } else if(t == typeof(Interval[])) {
                Span<Interval> span = ((Interval[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
            } else if(t == typeof(string[])) {
                Span<string> span = ((string[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
            } else if(t == typeof(Guid[])) {
                Span<Guid> span = ((Guid[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
            } else {
                elementsRead = 0;
                throw new NotSupportedException($"no PLAIN decoder exists for {t}");
            }
        }

        #region [ Single Value Encoding ]

        public static bool TryEncode(object? value, SchemaElement tse, out byte[]? result) {
            if(value == null) {
                result = null;
                return true;    // we've just successfully encoded null
            }

            System.Type t = value.GetType();
            if(t == typeof(bool)) {
                result = null;
                return true;
            } else if(t == typeof(byte)) {
                result = BitConverter.GetBytes((int)(byte)value);
                return true;
            } else if(t == typeof(sbyte)) {
                result = BitConverter.GetBytes((int)(sbyte)value);
                return true;
            } else if(t == typeof(short)) {
                result = BitConverter.GetBytes((int)(short)value);
                return true;
            } else if(t == typeof(ushort)) {
                result = BitConverter.GetBytes((int)(ushort)value);
                return true;
            } else if(t == typeof(int)) {
                result = BitConverter.GetBytes((int)value);
                return true;
            } else if(t == typeof(uint)) {
                result = BitConverter.GetBytes((int)(uint)value);
                return true;
            } else if(t == typeof(long)) {
                result = BitConverter.GetBytes((long)value);
                return true;
            } else if(t == typeof(ulong)) {
                result = BitConverter.GetBytes((ulong)value);
                return true;
            } else if(t == typeof(BigInteger)) {
                result = ((BigInteger)value).ToByteArray();
                return true;
            } else if(t == typeof(decimal))
                return TryEncode((decimal)value, tse, out result);
            else if(t == typeof(double)) {
                result = BitConverter.GetBytes((double)value);
                return true;
            } else if(t == typeof(float)) {
                result = BitConverter.GetBytes((float)value);
                return true;
            } else if(t == typeof(byte[])) {
                result = (byte[])value;
                return true;
            } else if(t == typeof(DateTime))
                return TryEncode((DateTime)value, tse, out result);
#if NET6_0_OR_GREATER
            else if(t == typeof(DateOnly))
                return TryEncode((DateOnly)value, tse, out result);
            else if(t == typeof(TimeOnly))
                return TryEncode((TimeOnly)value, tse, out result);
#endif
            else if(t == typeof(TimeSpan))
                return TryEncode((TimeSpan)value, tse, out result);
            else if(t == typeof(Interval)) {
                result = ((Interval)value).GetBytes();
                return true;
            } else if(t == typeof(string)) {
                result = System.Text.Encoding.UTF8.GetBytes((string)value);
                return true;
            }

            result = null;
            return false;
        }

        public static bool TryDecode(byte[]? value, SchemaElement tse, ParquetOptions? options,
            out object? result) {
            if(value == null) {
                result = null;
                return true;    // we've just successfully encoded null
            }

            if(tse.Type == TType.BOOLEAN) {
                result = BitConverter.ToBoolean(value, 0);
                return true;
            }
            if(tse.Type == TType.INT32) {
                result = BitConverter.ToInt32(value, 0);
                return true;
            } else if(tse.Type == TType.INT64) {
                result = BitConverter.ToInt64(value, 0);
                return true;
            } else if(tse.Type == TType.INT96) {
                if(value.Length == 12)
                    if(options?.TreatBigIntegersAsDates ?? false)
                        result = (DateTime)new NanoTime(value, 0);
                    else
                        result = new BigInteger(value);
                else
                    result = null;
                return true;
            } else if(tse.ConvertedType == ConvertedType.DECIMAL) {
                result = TryDecodeDecimal(value, tse);
                return true;
            } else if(tse.Type == TType.DOUBLE) {
                result = BitConverter.ToDouble(value, 0);
                return true;
            } else if(tse.Type == TType.FLOAT) {
                result = BitConverter.ToSingle(value, 0);
                return true;
            } else if(tse.Type == TType.BYTE_ARRAY)
                if(tse.ConvertedType != null && tse.ConvertedType == ConvertedType.UTF8) {
                    result = E.GetString(value);
                    return true;
                } else {
                    result = value;
                    return true;
                }

            result = null;
            return false;
        }

        private static decimal TryDecodeDecimal(byte[] value, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32:
                    decimal iscaleFactor = (decimal)Math.Pow(10, -tse.Scale!.Value);
                    int iv = BitConverter.ToInt32(value, 0);
                    decimal idv = iv * iscaleFactor;
                    return idv;
                case TType.INT64:
                    decimal lscaleFactor = (decimal)Math.Pow(10, -tse.Scale!.Value);
                    long lv = BitConverter.ToInt64(value, 0);
                    decimal ldv = lv * lscaleFactor;
                    return ldv;
                case TType.FIXED_LEN_BYTE_ARRAY:
                    return new BigDecimal(value, tse);
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
            }
        }

        private static bool TryEncode(DateTime value, SchemaElement tse, out byte[] result) {
            switch(tse.Type) {
                case TType.INT32:
                    int days = value.ToUnixDays();
                    result = BitConverter.GetBytes(days);
                    return true;
                case TType.INT64:
                    long unixTime = value.ToUtc().ToUnixMilliseconds();
                    result = BitConverter.GetBytes(unixTime);
                    return true;
                case TType.INT96:
                    var nano = new NanoTime(value.ToUtc());
                    result = nano.GetBytes();
                    return true;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");

            }
        }

#if NET6_0_OR_GREATER
        private static bool TryEncode(DateOnly value, SchemaElement tse, out byte[] result) {
            int days = value.ToUnixDays();
            result = BitConverter.GetBytes(days);
            return true;
        }

        private static bool TryEncode(TimeOnly value, SchemaElement tse, out byte[] result) {
            switch(tse.Type) {
                case TType.INT32:
                    int ms = (int)(value.Ticks / TimeSpan.TicksPerMillisecond);
                    result = BitConverter.GetBytes(ms);
                    return true;
                case TType.INT64:
                    long micros = value.Ticks / 10;
                    result = BitConverter.GetBytes(micros);
                    return true;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any time types");
            }
        }
#endif

        private static bool TryEncode(decimal value, SchemaElement tse, out byte[] result) {
            try {
                switch(tse.Type) {
                    case TType.INT32:
                        double sf32 = Math.Pow(10, tse.Scale!.Value);
                        int i = (int)(value * (decimal)sf32);
                        result = BitConverter.GetBytes(i);
                        return true;
                    case TType.INT64:
                        double sf64 = Math.Pow(10, tse.Scale!.Value);
                        long l = (long)(value * (decimal)sf64);
                        result = BitConverter.GetBytes(l);
                        return true;
                    case TType.FIXED_LEN_BYTE_ARRAY:
                        var bd = new BigDecimal(value, tse.Precision!.Value, tse.Scale!.Value);
                        result = bd.GetBytes();
                        return true;
                    default:
                        throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
                }
            } catch(OverflowException) {
                throw new ParquetException(
                   $"value '{value}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
            }
        }

        private static bool TryEncode(TimeSpan value, SchemaElement tse, out byte[] result) {
            switch(tse.Type) {
                case TType.INT32:
                    int ms = (int)value.TotalMilliseconds;
                    result = BitConverter.GetBytes(ms);
                    return true;
                case TType.INT64:
                    long micros = value.Ticks / 10;
                    result = BitConverter.GetBytes(micros);
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
                    if(flag)
                        b |= (byte)(1 << n);

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

            } finally {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        public static int Decode(Span<byte> source, Span<bool> data) {
            int tgtOffset = 0;
            int srcOffset = 0;

            int ibit = 0;
            while(srcOffset < source.Length && tgtOffset < data.Length) {
                byte b = source[srcOffset++];

                while(ibit < 8 && tgtOffset < data.Length && tgtOffset < data.Length) {
                    bool set = ((b >> ibit++) & 1) == 1;
                    data[tgtOffset++] = set;
                }

                ibit = 0;
            }

            return tgtOffset;
        }

        public static void Encode(ReadOnlySpan<byte> data, Stream destination, SchemaElement tse) {

            // copy shorts into ints
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                for(int i = 0; i < data.Length; i++)
                    ints[i] = data[i];
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }

            Encode(ints.AsSpan(0, data.Length), destination);
        }

        public static int Decode(Span<byte> source, Span<byte> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            int r = Decode(source, ints.AsSpan(0, data.Length));
            for(int i = 0; i < data.Length; i++)
                data[i] = (byte)ints[i];
            return r;
        }

        public static void Encode(ReadOnlySpan<sbyte> data, Stream destination) {

            // copy shorts into ints
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                for(int i = 0; i < data.Length; i++)
                    ints[i] = data[i];
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }

            Encode(ints.AsSpan(0, data.Length), destination);
        }

        public static int Decode(Span<byte> source, Span<sbyte> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                int r = Decode(source, ints.AsSpan(0, data.Length));
                for(int i = 0; i < data.Length; i++)
                    data[i] = (sbyte)ints[i];
                return r;
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }
        }

        public static void Encode(ReadOnlySpan<short> data, Stream destination) {

            // copy shorts into ints
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                for(int i = 0; i < data.Length; i++)
                    ints[i] = data[i];
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }

            Encode(ints.AsSpan(0, data.Length), destination);
        }

        public static int Decode(Span<byte> source, Span<short> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                int r = Decode(source, ints.AsSpan(0, data.Length));
                for(int i = 0; i < data.Length; i++)
                    data[i] = (short)ints[i];
                return r;
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }
        }

        public static void Encode(ReadOnlySpan<ushort> data, Stream destination) {

            // copy ushorts into ints
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                for(int i = 0; i < data.Length; i++)
                    ints[i] = data[i];
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }

            Encode(ints.AsSpan(0, data.Length), destination);
        }

        public static int Decode(Span<byte> source, Span<ushort> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                int r = Decode(source, ints.AsSpan(0, data.Length));
                for(int i = 0; i < data.Length; i++)
                    data[i] = (ushort)ints[i];
                return r;
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }
        }

        public static void Encode(ReadOnlySpan<int> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static void Encode(ReadOnlySpan<uint> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static void Encode(ReadOnlySpan<long> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static void Encode(ReadOnlySpan<ulong> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static void Encode(ReadOnlySpan<BigInteger> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode<T>(Span<byte> source, Span<T> data) where T : struct {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            source.CopyWithLimitTo(bytes);
            return Math.Min(data.Length, source.Length / Unsafe.SizeOf<T>());
        }

        public static void Encode(ReadOnlySpan<decimal> data, Stream destination, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32:
                    double scaleFactor32 = Math.Pow(10, tse.Scale ?? 0);
                    foreach(decimal d in data)
                        try {
                            int i = (int)(d * (decimal)scaleFactor32);
                            byte[] b = BitConverter.GetBytes(i);
                            destination.Write(b, 0, b.Length);
                        } catch(OverflowException) {
                            throw new ParquetException(
                               $"value '{d}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
                        }
                    break;
                case TType.INT64:
                    double sf64 = Math.Pow(10, tse.Scale ?? 0);
                    foreach(decimal d in data)
                        try {
                            long l = (long)(d * (decimal)sf64);
                            byte[] b = BitConverter.GetBytes(l);
                            destination.Write(b, 0, b.Length);
                        } catch(OverflowException) {
                            throw new ParquetException(
                               $"value '{d}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
                        }
                    break;
                case TType.FIXED_LEN_BYTE_ARRAY:
                    foreach(decimal d in data) {
                        var bd = new BigDecimal(d, tse.Precision ?? 0, tse.Scale ?? 0);
                        byte[] b = bd.GetBytes();
                        tse.TypeLength = b.Length; //always re-set type length as it can differ from default type length
                        destination.Write(b, 0, b.Length);
                    }
                    break;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
            }
        }

        public static int Decode(Span<byte> source, Span<decimal> data, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32: {
                        decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale ?? 0);
                        int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
                        try {
                            Decode(source, ints.AsSpan(0, data.Length));
                            for(int i = 0; i < data.Length; i++)
                                data[i] = ints[i] * scaleFactor;
                            return data.Length;
                        } finally {
                            ArrayPool<int>.Shared.Return(ints);
                        }
                    }
                case TType.INT64: {
                        decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale ?? 0);
                        long[] longs = ArrayPool<long>.Shared.Rent(data.Length);
                        try {
                            Decode(source, longs.AsSpan(0, data.Length));
                            for(int i = 0; i < data.Length; i++)
                                data[i] = longs[i] * scaleFactor;
                            return data.Length;
                        } finally {
                            ArrayPool<long>.Shared.Return(longs);
                        }
                    }
                case TType.FIXED_LEN_BYTE_ARRAY: {
                        int tl = tse.TypeLength ?? 0;
                        if(tl == 0)
                            return 0;

                        byte[] chunk = new byte[tl];
                        int i = 0;
                        for(int offset = 0; offset + tl <= source.Length && i < data.Length; offset += tl, i++) {
                            decimal dc = new BigDecimal(source.Slice(offset, tl).ToArray(), tse);
                            data[i] = dc;
                        }
                        return i;
                    }
                case TType.BYTE_ARRAY: {
                        // type_length: 0
                        // precision: 4
                        // scale: 2
                        // see https://github.com/apache/arrow/pull/2646/files

                        int read = 0;
                        // convert each byte chunk to valid decimal bytes
                        int spos = 0;
                        while(read < data.Length) {
                            int length = source.ReadInt32(spos);
                            spos += sizeof(int);
                            if(length > 0) {
                                decimal dc = new BigDecimal(source.Slice(spos, length).ToArray(), tse);
                                spos += length;
                                data[read++] = dc;
                            }
                        }
                        return read;
                    }

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

        public static int Decode(Span<byte> source, Span<float> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            source.CopyWithLimitTo(bytes);
            return data.Length;
        }

        public static void Encode(ReadOnlySpan<byte[]> data, Stream destination) {
            foreach(byte[] element in data) {
                byte[] l = BitConverter.GetBytes(element.Length);
                destination.Write(l, 0, l.Length);
                destination.Write(element, 0, element.Length);
            }
        }

        public static void Encode(ReadOnlySpan<Guid> data, Stream destination) {
            foreach(Guid element in data) {
                byte[] b = element.ToBigEndianByteArray();
                destination.Write(b, 0, b.Length);
            }
        }

        public static int Decode(Span<byte> source, Span<byte[]> data, SchemaElement tse) {
            int read = 0;
            int sourceOffset = 0;

            if(tse.Type == TType.FIXED_LEN_BYTE_ARRAY) {
                if(tse.TypeLength == null)
                    throw new InvalidDataException($"type length must be set for {nameof(TType.FIXED_LEN_BYTE_ARRAY)}");
                int length = tse.TypeLength.Value;
                while(read < data.Length) {
                    byte[] el = source.Slice(sourceOffset, length).ToArray();
                    sourceOffset += length;
                    data[read++] = el;
                }
            } else {
                while(read < data.Length) {
                    int length = source.ReadInt32(sourceOffset);
                    sourceOffset += sizeof(int);
                    if(length > 0) {
                        byte[] el = source.Slice(sourceOffset, length).ToArray();
                        sourceOffset += length;
                        data[read++] = el;
                    }
                }
            }
            return read;
        }

        public static void Encode(ReadOnlySpan<DateTime> data, Stream destination, SchemaElement tse) {

            switch(tse.Type) {
                case TType.INT32:
                    foreach(DateTime element in data) {
                        int days = element.ToUnixDays();
                        byte[] raw = BitConverter.GetBytes(days);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case TType.INT64:
                    if(tse.LogicalType?.TIMESTAMP is not null) {
                        foreach(DateTime element in data) {
                            if(tse.LogicalType.TIMESTAMP.Unit.MILLIS is not null) {
                                long unixTime = element.ToUnixMilliseconds();
                                byte[] raw = BitConverter.GetBytes(unixTime);
                                destination.Write(raw, 0, raw.Length);    
#if NET7_0_OR_GREATER
                            } else if (tse.LogicalType.TIMESTAMP.Unit.MICROS is not null) {
                                long unixTime = element.ToUtc().ToUnixMicroseconds();
                                byte[] raw = BitConverter.GetBytes(unixTime);
                                destination.Write(raw, 0, raw.Length);
                            } else if (tse.LogicalType.TIMESTAMP.Unit.NANOS is not null) {
                                long unixTime = element.ToUtc().ToUnixNanoseconds();
                                byte[] raw = BitConverter.GetBytes(unixTime);
                                destination.Write(raw, 0, raw.Length);
#endif
                            } else {
                                throw new ParquetException($"Unexpected TimeUnit: {tse.LogicalType.TIMESTAMP.Unit}");
                            }
                        }
                    } else if(tse.ConvertedType == ConvertedType.TIMESTAMP_MILLIS) {
                        foreach(DateTime element in data) {
                            long unixTime = element.ToUtc().ToUnixMilliseconds();
                            byte[] raw = BitConverter.GetBytes(unixTime);
                            destination.Write(raw, 0, raw.Length);
                        }
#if NET7_0_OR_GREATER
                    } else if(tse.ConvertedType == ConvertedType.TIMESTAMP_MICROS) {
                        foreach(DateTime element in data) {
                            long unixTime = element.ToUtc().ToUnixMicroseconds();
                            byte[] raw = BitConverter.GetBytes(unixTime);
                            destination.Write(raw, 0, raw.Length);
                        }
#endif
                    } else {
                        throw new ArgumentException($"invalid converted type: {tse.ConvertedType}");
                    }
                    break;
                case TType.INT96:
                    foreach(DateTime element in data) {
                        var nano = new NanoTime(element.ToUtc());
                        nano.Write(destination);
                    }
                    break;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");

            }
        }

#if NET6_0_OR_GREATER
        public static void Encode(ReadOnlySpan<DateOnly> data, Stream destination, SchemaElement tse) {
            foreach(DateOnly element in data) {
                int days = element.ToUnixDays();
                byte[] raw = BitConverter.GetBytes(days);
                destination.Write(raw, 0, raw.Length);
            }
        }
        public static void Encode(ReadOnlySpan<TimeOnly> data, Stream destination, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32:
                    foreach(TimeOnly element in data) {
                        int ticks = (int)(element.Ticks / TimeSpan.TicksPerMillisecond);
                        byte[] raw = BitConverter.GetBytes(ticks);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case TType.INT64:
                    foreach(TimeOnly element in data) {
                        long ticks = element.Ticks / 10;
                        byte[] raw = BitConverter.GetBytes(ticks);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
            }
        }
#endif

        public static int Decode(Span<byte> source, Span<DateTime> data, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32:
                    int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
                    try {
                        int intsRead = Decode(source, ints.AsSpan(0, data.Length));
                        for(int i = 0; i < intsRead; i++)
                            data[i] = ints[i].AsUnixDaysInDateTime();
                        return intsRead;
                    } finally {
                        ArrayPool<int>.Shared.Return(ints);
                    }
                case TType.INT64:
                    long[] longs = ArrayPool<long>.Shared.Rent(data.Length);
                    try {
                        int longsRead = Decode(source, longs.AsSpan(0, data.Length));
                        if(tse.LogicalType?.TIMESTAMP is not null) {
                            for(int i = 0; i < longsRead; i++) {
                                if(tse.LogicalType.TIMESTAMP.Unit.MILLIS is not null) {
                                    DateTime dt = longs[i].AsUnixMillisecondsInDateTime();
                                    dt = DateTime.SpecifyKind(dt, tse.LogicalType.TIMESTAMP.IsAdjustedToUTC ? DateTimeKind.Utc : DateTimeKind.Local);
                                    data[i] = dt;
                                } else if(tse.LogicalType.TIMESTAMP.Unit.MICROS is not null) {
                                    long lv = longs[i];
                                    long microseconds = lv % 1000;
                                    lv /= 1000;
                                    DateTime dt = lv.AsUnixMillisecondsInDateTime().AddTicks(microseconds * 10);
                                    dt = DateTime.SpecifyKind(dt, tse.LogicalType.TIMESTAMP.IsAdjustedToUTC ? DateTimeKind.Utc : DateTimeKind.Local);
                                    data[i] = dt;
                                } else if(tse.LogicalType.TIMESTAMP.Unit.NANOS is not null) {
                                    long lv = longs[i];
                                    long nanoseconds = lv % 1000000;
                                    lv /= 1000000;
                                    DateTime dt = lv.AsUnixMillisecondsInDateTime().AddTicks(nanoseconds / 100); // 1 tick = 100 nanoseconds
                                    dt = DateTime.SpecifyKind(dt, tse.LogicalType.TIMESTAMP.IsAdjustedToUTC ? DateTimeKind.Utc : DateTimeKind.Local);
                                    data[i] = dt;
                                } else {
                                    throw new ParquetException($"Unexpected TimeUnit: {tse.LogicalType.TIMESTAMP.Unit}");
                                }
                            }
                        } else if(tse.ConvertedType == ConvertedType.TIMESTAMP_MICROS) {
                            for(int i = 0; i < longsRead; i++) {
                                long lv = longs[i];
                                long microseconds = lv % 1000;
                                lv /= 1000;
                                data[i] = lv.AsUnixMillisecondsInDateTime().AddTicks(microseconds * 10);
                            }
                        } else {
                            for(int i = 0; i < longsRead; i++)
                                data[i] = longs[i].AsUnixMillisecondsInDateTime();
                        }
                        return longsRead;
                    } finally {
                        ArrayPool<long>.Shared.Return(longs);
                    }
                case TType.INT96:
                    byte[] buf = ArrayPool<byte>.Shared.Rent(NanoTime.BinarySize * data.Length);
                    try {
                        int offset = 0;
                        int els = 0;
                        while(offset + NanoTime.BinarySize <= source.Length) {
                            data[els++] = new NanoTime(source.Slice(offset, NanoTime.BinarySize));
                            offset += NanoTime.BinarySize;
                        }
                        return els;
                    } finally {
                        ArrayPool<byte>.Shared.Return(buf);
                    }
                default:
                    throw new NotSupportedException();
            }
        }

#if NET6_0_OR_GREATER
        public static int Decode(Span<byte> source, Span<DateOnly> data, SchemaElement tse) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                int intsRead = Decode(source, ints.AsSpan(0, data.Length));
                for(int i = 0; i < intsRead; i++)
                    data[i] = DateOnly.FromDateTime(ints[i].AsUnixDaysInDateTime());
                return intsRead;
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }
        }

        public static int Decode(Span<byte> source, Span<TimeOnly> data, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32: {
                    int i = 0;
                    int srcPos = 0;
                    while(srcPos + sizeof(int) <= source.Length && i < data.Length) {
                        int iv = source.ReadInt32(srcPos);
                        srcPos += sizeof(int);
                        data[i++] = new TimeOnly(iv * TimeSpan.TicksPerMillisecond);
                    }
                    return i;
                }
                case TType.INT64: {
                    int i = 0;
                    int srcPos = 0;
                    while(srcPos + sizeof(long) <= source.Length && i < data.Length) {
                        long lv = source.ReadInt64(srcPos);
                        srcPos += sizeof(long);
                        data[i++] = new TimeOnly(lv * 10);
                    }
                    return i;
                }
                default:
                    throw new NotSupportedException();
            }
        }
#endif

        public static void Encode(ReadOnlySpan<TimeSpan> data, Stream destination, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32:
                    foreach(TimeSpan ts in data) {
                        int ms = (int)ts.TotalMilliseconds;
                        byte[] raw = BitConverter.GetBytes(ms);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case TType.INT64:
                    foreach(TimeSpan ts in data) {
                        long micros = ts.Ticks / 10;
                        byte[] raw = BitConverter.GetBytes(micros);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent any date types");
            }
        }

        public static int Decode(Span<byte> source, Span<TimeSpan> data, SchemaElement tse) {
            switch(tse.Type) {
                case TType.INT32: {
                        int i = 0;
                        int srcPos = 0;
                        while(srcPos + sizeof(int) <= source.Length && i < data.Length) {
                            int iv = source.ReadInt32(srcPos);
                            srcPos += sizeof(int);
                            data[i++] = new TimeSpan(0, 0, 0, 0, iv);
                        }
                        return i;
                    }
                case TType.INT64: {
                        int i = 0;
                        int srcPos = 0;
                        while(srcPos + sizeof(long) <= source.Length && i < data.Length) {
                            long lv = source.ReadInt64(srcPos);
                            srcPos += sizeof(long);
                            data[i++] = new TimeSpan(lv * 10);
                        }
                        return i;
                    }
                default:
                    throw new NotSupportedException();
            }
        }

        public static void Encode(ReadOnlySpan<Interval> data, Stream destination) {
            foreach(Interval iv in data) {
                byte[] b = iv.GetBytes();
                destination.Write(b, 0, b.Length);
            }
        }

        public static int Decode(Span<byte> source, Span<Interval> data) {
            int i = 0;
            int pos = 0;
            while(pos + Interval.BinarySize <= source.Length && i < data.Length) {
                int months = source.ReadInt32(pos);
                pos += sizeof(int);
                int days = source.ReadInt32(pos);
                pos += sizeof(int);
                int millis = source.ReadInt32(pos);
                pos += sizeof(int);
                var e = new Interval(months, days, millis);

                data[i++] = e;
            }
            return i;
        }

        public static void Encode(ReadOnlySpan<string> data, Stream destination) {

            // rent a buffer large enough not to reallocate often and not call stream write often
            byte[] rb = BytePool.Rent(1024 * 10);
            int rbOffset = 0;
            try {

                foreach(string s in data) {
                    int len = string.IsNullOrEmpty(s) ? 0 : E.GetByteCount(s);
                    int minLen = len + sizeof(int);
                    int rem = rb.Length - rbOffset;

                    // check we have enough space left
                    if(rem < minLen) {
                        destination.Write(rb, 0, rbOffset); // dump current buffer
                        rbOffset = 0;

                        // do we need to reallocate for more space?
                        if(minLen > rb.Length) {
                            BytePool.Return(rb);
                            rb = BytePool.Rent(minLen);
                        }
                    }

                    // write our data
                    if(len == 0)
                        Array.Copy(ZeroInt32, 0, rb, rbOffset, ZeroInt32.Length);
                    else
                        Array.Copy(BitConverter.GetBytes(len), 0, rb, rbOffset, sizeof(int));
                    rbOffset += sizeof(int);
                    if(len > 0) {
                        E.GetBytes(s, 0, s.Length, rb, rbOffset);
                        rbOffset += len;
                    }
                }

                if(rbOffset > 0)
                    destination.Write(rb, 0, rbOffset);

            } finally {
                BytePool.Return(rb);
            }
        }

        public static int Decode(Span<byte> source, Span<string> data, SchemaElement tse) {
            //int remLength = (int)(source.Length - source.Position);

            if(source.Length == 0)
                return 0;

            int i = 0;

            if(tse.Type == TType.FIXED_LEN_BYTE_ARRAY) {
                if(tse.TypeLength == null)
                    throw new InvalidDataException($"type length must be set for {nameof(TType.FIXED_LEN_BYTE_ARRAY)}");
                int length = tse.TypeLength.Value;

                for(int spanIdx = 0; spanIdx < source.Length && i < data.Length; i++) {
#if NETSTANDARD2_0
                    data[i] = E.GetString(source.Slice(spanIdx, length).ToArray());
#else
                    data[i] = E.GetString(source.Slice(spanIdx, length));
#endif
                    spanIdx += length;
                }

            } else {

                for(int spanIdx = 0; spanIdx < source.Length && i < data.Length; i++) {
                    int length = source.ReadInt32(spanIdx);
                    spanIdx += sizeof(int);
#if NETSTANDARD2_0
                data[i] = E.GetString(source.Slice(spanIdx, length).ToArray());
#else
                    data[i] = E.GetString(source.Slice(spanIdx, length));
#endif
                    spanIdx += length;
                }
            }
            return i;
        }

        public static int Decode(Span<byte> source, Span<Guid> data, SchemaElement tse) {
            if(tse.TypeLength != 16)
                throw new InvalidOperationException($"'{tse.TypeLength}' is invalid type length for UUID (should be 16)");

            int i = 0;
            for(int offset = 0; offset + 16 <= source.Length && i < data.Length; offset += 16, i++) {
                data[i] = ((ReadOnlySpan<byte>)source.Slice(offset, 16)).ToGuidFromBigEndian();
            }
            return i;
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

        public static void FillStats(ReadOnlySpan<ushort> data, DataColumnStatistics stats) {
            data.MinMax(out ushort min, out ushort max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<int> data, DataColumnStatistics stats) {
            data.MinMax(out int min, out int max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<uint> data, DataColumnStatistics stats) {
            data.MinMax(out uint min, out uint max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<long> data, DataColumnStatistics stats) {
            data.MinMax(out long min, out long max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<ulong> data, DataColumnStatistics stats) {
            data.MinMax(out ulong min, out ulong max);
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

#if NET6_0_OR_GREATER
        public static void FillStats(ReadOnlySpan<DateOnly> data, DataColumnStatistics stats) {
            data.MinMax(out DateOnly min, out DateOnly max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<TimeOnly> data, DataColumnStatistics stats) {
            data.MinMax(out TimeOnly min, out TimeOnly max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }
#endif

        public static void FillStats(ReadOnlySpan<TimeSpan> data, DataColumnStatistics stats) {
            data.MinMax(out TimeSpan min, out TimeSpan max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        public static void FillStats(ReadOnlySpan<string> data, DataColumnStatistics stats) {
            data.MinMax(out string? min, out string? max);
            stats.MinValue = min;
            stats.MaxValue = max;
        }

        #endregion
    }
}