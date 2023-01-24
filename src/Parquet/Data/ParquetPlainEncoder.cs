using System;
using System.Buffers;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Parquet.Extensions;
using Parquet.File.Values.Primitives;
using Parquet.Thrift;

namespace Parquet.Data {

    /// <summary>
    /// Fast data encoder.
    /// Experimental.
    /// </summary>
    static class ParquetPlainEncoder {

        private static readonly System.Text.Encoding E = System.Text.Encoding.UTF8;
        private static readonly byte[] ZeroInt32 = BitConverter.GetBytes((int)0);
        private static readonly ArrayPool<byte> BytePool = ArrayPool<byte>.Shared;

        public static bool Encode(
            Array data, int offset, int count,
            Thrift.SchemaElement tse,
            Stream destination,
            DataColumnStatistics? stats = null) {
            System.Type t = data.GetType();

            if(t == typeof(bool[])) {
                Span<bool> span = ((bool[])data).AsSpan(offset, count);
                Encode(span, destination);
                // no stats for bools
                return true;
            }
            else if(t == typeof(byte[])) {
                Span<byte> span = ((byte[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
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
            else if(t == typeof(UInt16[])) {
                Span<ushort> span = ((ushort[])data).AsSpan(offset, count);
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
            else if(t == typeof(UInt32[])) {
                Span<uint> span = ((uint[])data).AsSpan(offset, count);
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
            else if(t == typeof(UInt64[])) {
                Span<ulong> span = ((ulong[])data).AsSpan(offset, count);
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
            }
            else if(t == typeof(double[])) {
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
            else if(t == typeof(TimeSpan[])) {
                Span<TimeSpan> span = ((TimeSpan[])data).AsSpan(offset, count);
                Encode(span, destination, tse);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }
            else if(t == typeof(Interval[])) {
                Span<Interval> span = ((Interval[])data).AsSpan(offset, count);
                Encode(span, destination);
                // no stats, maybe todo
                return true;
            }
            else if(t == typeof(string[])) {
                Span<string> span = ((string[])data).AsSpan(offset, count);
                Encode(span, destination);
                if(stats != null) {
                    FillStats(span, stats);
                }
                return true;
            }

            return false;
        }

        // todo: instead of Stream, use Span<byte>, because we already read it all into memorystream!

        public static bool Decode(
            Array data, int offset, int count,
            Thrift.SchemaElement tse,
            Stream source,
            out int elementsRead) {

            int rem = data.Length - offset;
            if(count > rem)
                count = rem;

            System.Type t = data.GetType();

            if(t == typeof(bool[])) {
                elementsRead = Decode(source, ((bool[])data).AsSpan(offset, count));
                return true;
            }
            else if(t == typeof(byte[])) {
                elementsRead = Decode(source, ((byte[])data).AsSpan(offset, count));
                return true;
            }
            else if(t == typeof(sbyte[])) {
                elementsRead = Decode(source, ((sbyte[])data).AsSpan(offset, count));
                return true;
            }
            else if(t == typeof(Int16[])) {
                elementsRead = Decode(source, ((Int16[])data).AsSpan(offset, count));
                return true;

            }
            else if(t == typeof(UInt16[])) {
                elementsRead = Decode(source, ((UInt16[])data).AsSpan(offset, count));
                return true;
            }
            else if(t == typeof(Int32[])) {
                Span<int> span = ((int[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(UInt32[])) {
                Span<uint> span = ((uint[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(Int64[])) {
                Span<long> span = ((long[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(UInt64[])) {
                Span<ulong> span = ((ulong[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(BigInteger[])) {
                Span<BigInteger> span = ((BigInteger[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(decimal[])) {
                Span<decimal> span = ((decimal[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
                return true;
            }
            else if(t == typeof(double[])) {
                Span<double> span = ((double[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;

            }
            else if(t == typeof(float[])) {
                Span<float> span = ((float[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(byte[][])) {
                Span<byte[]> span = ((byte[][])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(DateTime[])) {
                Span<DateTime> span = ((DateTime[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
                return true;
            }
            else if(t == typeof(TimeSpan[])) {
                Span<TimeSpan> span = ((TimeSpan[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
                return true;
            }
            else if(t == typeof(Interval[])) {
                Span<Interval> span = ((Interval[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span);
                return true;
            }
            else if(t == typeof(string[])) {
                Span<string> span = ((string[])data).AsSpan(offset, count);
                elementsRead = Decode(source, span, tse);
                return true;
            }

            elementsRead = 0;
            return false;
        }

        #region [ Single Value Encoding ]

        public static bool TryEncode(object? value, Thrift.SchemaElement tse, out byte[]? result) {
            if(value == null) {
                result = null;
                return true;    // we've just successfully encoded null
            }

            System.Type t = value.GetType();
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
            else if(t == typeof(UInt16)) {
                result = BitConverter.GetBytes((int)(ushort)value);
                return true;
            }
            else if(t == typeof(Int32)) {
                result = BitConverter.GetBytes((int)value);
                return true;
            }
            else if(t == typeof(UInt32)) {
                result = BitConverter.GetBytes((int)(uint)value);
                return true;
            }
            else if(t == typeof(Int64)) {
                result = BitConverter.GetBytes((long)value);
                return true;
            }
            else if(t == typeof(UInt64)) {
                result = BitConverter.GetBytes((ulong)value);
                return true;
            }
            else if(t == typeof(BigInteger)) {
                result = ((BigInteger)value).ToByteArray();
                return true;
            }
            else if(t == typeof(decimal)) {
                return TryEncode((decimal)value, tse, out result);
            }
            else if(t == typeof(double)) {
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
            else if(t == typeof(DateTime)) {
                return TryEncode((DateTime)value, tse, out result);
            }
            else if(t == typeof(TimeSpan)) {
                return TryEncode((TimeSpan)value, tse, out result);
            }
            else if(t == typeof(Interval)) {
                result = ((Interval)value).GetBytes();
                return true;
            }
            else if(t == typeof(string)) {
                result = System.Text.Encoding.UTF8.GetBytes((string)value);
                return true;
            }

            result = null;
            return false;
        }

        public static bool TryDecode(byte[]? value, Thrift.SchemaElement tse, out object? result) {
            if(value == null) {
                result = null;
                return true;    // we've just successfully encoded null
            }

            if(tse.Type == Thrift.Type.BOOLEAN) {
                result = BitConverter.ToBoolean(value, 0);
                return true;
            }
            if(tse.Type == Thrift.Type.INT32) {
                result = BitConverter.ToInt32(value, 0);
                return true;
            }
            else if(tse.Type == Thrift.Type.INT64) {
                result = BitConverter.ToInt64(value, 0);
                return true;
            }
            else if(tse.Type == Thrift.Type.INT96) {
                if(value.Length == 12) {
                    if(tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DATE) {
                        result = (DateTime)new NanoTime(value, 0);
                    }
                    else {
                        result = new BigInteger(value);
                    }
                }
                else {
                    result = null;
                }
                return true;
            }
            else if(tse.Converted_type == Thrift.ConvertedType.DECIMAL) {
                result = TryDecodeDecimal(value, tse);
                return true;
            }
            else if(tse.Type == Thrift.Type.DOUBLE) {
                result = BitConverter.ToDouble(value, 0);
                return true;
            }
            else if(tse.Type == Thrift.Type.FLOAT) {
                result = BitConverter.ToSingle(value, 0);
                return true;
            }
            else if(tse.Type == Thrift.Type.BYTE_ARRAY) {
                if(tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.UTF8) {
                    result = E.GetString(value);
                    return true;
                } else {
                    result = value;
                    return true;
                }
            }

            result = null;
            return false;
        }

        private static decimal TryDecodeDecimal(byte[] value, Thrift.SchemaElement tse) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    decimal iscaleFactor = (decimal)Math.Pow(10, -tse.Scale);
                    int iv = BitConverter.ToInt32(value, 0);
                    decimal idv = iv * iscaleFactor;
                    return idv;
                case Thrift.Type.INT64:
                    decimal lscaleFactor = (decimal)Math.Pow(10, -tse.Scale);
                    long lv = BitConverter.ToInt64(value, 0);
                    decimal ldv = lv * lscaleFactor;
                    return ldv;
                case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
                    return new BigDecimal(value, tse);
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
            }
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

        private static bool TryEncode(TimeSpan value, Thrift.SchemaElement tse, out byte[] result) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    int ms = (int)value.TotalMilliseconds;
                    result = BitConverter.GetBytes(ms);
                    return true;
                case Thrift.Type.INT64:
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

        public static int Decode(Stream source, Span<bool> data) {
            int offset = 0;

            int ibit = 0;
            while(source.Position < source.Length && offset < data.Length) {
                byte b = (byte)source.ReadByte();

                while(ibit < 8 && offset < data.Length) {
                    bool set = ((b >> ibit++) & 1) == 1;
                    data[offset++] = set;
                }

                ibit = 0;
            }

            return offset;
        }

        public static void Encode(ReadOnlySpan<byte> data, Stream destination, Thrift.SchemaElement tse) {

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

        public static int Decode(Stream source, Span<byte> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            int r = Decode(source, ints.AsSpan(0, data.Length));
            for(int i = 0; i < data.Length; i++) {
                data[i] = (byte)ints[i];
            }
            return r;
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

        public static int Decode(Stream source, Span<sbyte> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                int r = Decode(source, ints.AsSpan(0, data.Length));
                for(int i = 0; i < data.Length; i++) {
                    data[i] = (sbyte)ints[i];
                }
                return r;
            } finally {
                ArrayPool<int>.Shared.Return(ints);
            }
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

        public static int Decode(Stream source, Span<short> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                int r = Decode(source, ints.AsSpan(0, data.Length));
                for(int i = 0; i < data.Length; i++) {
                    data[i] = (short)ints[i];
                }
                return r;
            }
            finally {
                ArrayPool<int>.Shared.Return(ints);
            }
        }

        public static void Encode(ReadOnlySpan<ushort> data, Stream destination) {

            // copy ushorts into ints
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

        public static int Decode(Stream source, Span<ushort> data) {
            int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
            try {
                int r = Decode(source, ints.AsSpan(0, data.Length));
                for(int i = 0; i < data.Length; i++) {
                    data[i] = (ushort)ints[i];
                }
                return r;
            }
            finally {
                ArrayPool<int>.Shared.Return(ints);
            }
        }

        public static void Encode(ReadOnlySpan<int> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode(Stream source, Span<int> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes) / sizeof(int);
        }

        public static void Encode(ReadOnlySpan<uint> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode(Stream source, Span<uint> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes) / sizeof(uint);
        }

        public static void Encode(ReadOnlySpan<long> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode(Stream source, Span<long> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes) / sizeof(long);
        }

        public static void Encode(ReadOnlySpan<ulong> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode(Stream source, Span<ulong> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes) / sizeof(ulong);
        }

        public static void Encode(ReadOnlySpan<BigInteger> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode(Stream source, Span<BigInteger> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes) / 12;
        }

        public static void Encode(ReadOnlySpan<decimal> data, Stream destination, Thrift.SchemaElement tse) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    double scaleFactor32 = Math.Pow(10, tse.Scale);
                    foreach(decimal d in data) {
                        try {
                            int i = (int)(d * (decimal)scaleFactor32);
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

        public static int Decode(Stream source, Span<decimal> data, Thrift.SchemaElement tse) {
            switch(tse.Type) {
                case Thrift.Type.INT32: {
                        decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
                        int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
                        try {
                            Decode(source, ints.AsSpan(0, data.Length));
                            for(int i = 0; i < data.Length; i++) {
                                data[i] = ints[i] * scaleFactor;
                            }
                            return data.Length;
                        } finally {
                            ArrayPool<int>.Shared.Return(ints);
                        }
                    }
                case Thrift.Type.INT64: {
                        decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
                        long[] longs = ArrayPool<long>.Shared.Rent(data.Length);
                        try {
                            Decode(source, longs.AsSpan(0, data.Length));
                            for(int i = 0; i < data.Length; i++) {
                                data[i] = longs[i] * scaleFactor;
                            }
                            return data.Length;
                        }
                        finally {
                            ArrayPool<long>.Shared.Return(longs);
                        }
                    }
                case Thrift.Type.FIXED_LEN_BYTE_ARRAY: {
                        int tl = tse.Type_length;
                        if(tl == 0)
                            return 0;

                        byte[] raw = ArrayPool<byte>.Shared.Rent(data.Length * tl);
                        Read(source, raw.AsSpan(0, data.Length * tl));
                        try {
                            byte[] chunk = new byte[tl];
                            for(int offset = 0, i = 0; offset + tl <= data.Length * tl; offset += tl, i ++) {
                                Array.Copy(raw, offset, chunk, 0, tl);
                                decimal dc = new BigDecimal(chunk, tse);
                                data[i] = dc;
                            }
                            return data.Length;
                        }
                        finally {
                            ArrayPool<byte>.Shared.Return(raw);
                        }

                    }
                case Thrift.Type.BYTE_ARRAY: {
                        // type_length: 0
                        // precision: 4
                        // scale: 2
                        // see https://github.com/apache/arrow/pull/2646/files

                        int read = 0;
                        // convert each byte chunk to valid decimal bytes
                        while(read < data.Length) {
                            int length = source.ReadInt32();
                            if(length > 0) {
                                byte[] raw = ArrayPool<byte>.Shared.Rent(length);
                                Span<byte> span = raw.AsSpan(0, length);
                                Read(source, span);
                                span.Reverse();
                                decimal dc = new BigDecimal(span.ToArray(), tse, true);
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

        public static int Decode(Stream source, Span<double> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes) / sizeof(double);
        }

        public static void Encode(ReadOnlySpan<float> data, Stream destination) {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(data);
            Write(destination, bytes);
        }

        public static int Decode(Stream source, Span<float> data) {
            Span<byte> bytes = MemoryMarshal.AsBytes(data);
            return Read(source, bytes) / sizeof(float);
        }

        public static void Encode(ReadOnlySpan<byte[]> data, Stream destination) {
            foreach(byte[] element in data) {
                byte[] l = BitConverter.GetBytes(element.Length);
                destination.Write(l, 0, l.Length);
                destination.Write(element, 0, element.Length);
            }
        }

        public static int Decode(Stream source, Span<byte[]> data) {
            int read = 0;

            while(read < data.Length) {
                int length = source.ReadInt32();
                if(length > 0) {
                    byte[] el = new byte[length];
                    int elRead = source.Read(el, 0, length);
                    data[read++] = el;
                    if(elRead != length)
                        throw new IOException($"expected {length} but read {elRead}");
                }
            }
            return read;
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

        public static int Decode(Stream source, Span<DateTime> data, Thrift.SchemaElement tse) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    int[] ints = ArrayPool<int>.Shared.Rent(data.Length);
                    try {
                        int intsRead = Decode(source, ints.AsSpan(0, data.Length));
                        for(int i = 0; i < intsRead; i++) {
                            data[i] = ints[i].AsUnixDaysInDateTime();
                        }
                        return intsRead;
                    } finally {
                        ArrayPool<int>.Shared.Return(ints);
                    }
                case Thrift.Type.INT64:
                    long[] longs = ArrayPool<long>.Shared.Rent(data.Length);
                    try {
                        int longsRead = Decode(source, longs.AsSpan(0, data.Length));
                        bool isMicros = tse.__isset.converted_type &&
                                        tse.Converted_type == ConvertedType.TIMESTAMP_MICROS;
                        if(isMicros) {
                            for(int i = 0; i < longsRead; i++) {
                                long lv = longs[i];
                                long microseconds = lv % 1000;
                                lv /= 1000;
                                data[i] = lv.AsUnixMillisecondsInDateTime().AddTicks(microseconds * 10);
                            }
                        }
                        else {
                            for(int i = 0; i < longsRead; i++) {
                                data[i] = longs[i].AsUnixMillisecondsInDateTime();
                            }
                        }
                        return longsRead;
                    }
                    finally {
                        ArrayPool<long>.Shared.Return(longs);
                    }
                case Thrift.Type.INT96:
                    byte[] buf = ArrayPool<byte>.Shared.Rent(NanoTime.BinarySize * data.Length);
                    try {
                        Span<byte> span = buf.AsSpan(0, NanoTime.BinarySize * data.Length);
                        Read(source, span);
                        int offset = 0;
                        int els = 0;
                        while(offset + NanoTime.BinarySize <= span.Length) {
                            data[els++] = new NanoTime(span.Slice(offset, NanoTime.BinarySize));
                            offset += NanoTime.BinarySize;
                        }
                        return els;
                    }finally {
                        ArrayPool<byte>.Shared.Return(buf);
                    }
                default:
                    throw new NotSupportedException();
            }
        }

        public static void Encode(ReadOnlySpan<TimeSpan> data, Stream destination, Thrift.SchemaElement tse) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    foreach(TimeSpan ts in data) {
                        int ms = (int)ts.TotalMilliseconds;
                        byte[] raw = BitConverter.GetBytes(ms);
                        destination.Write(raw, 0, raw.Length);
                    }
                    break;
                case Thrift.Type.INT64:
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

        public static int Decode(Stream source, Span<TimeSpan> data, Thrift.SchemaElement tse) {
            switch(tse.Type) {
                case Thrift.Type.INT32: {
                        int i = 0;
                        while(source.Position + 4 <= source.Length) {
                            int iv = source.ReadInt32();
                            data[i++] = new TimeSpan(0, 0, 0, 0, iv);
                        }
                        return i;
                    }
                case Thrift.Type.INT64: {
                        int i = 0;
                        while(source.Position + 8 <= source.Length) {
                            long lv = source.ReadInt64();
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

        public static int Decode(Stream source, Span<Interval> data) {
            int i = 0;
            while(source.Position + Interval.BinarySize <= source.Length) {
                int months = source.ReadInt32();
                int days = source.ReadInt32();
                int millis = source.ReadInt32();
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
                    if(len == 0) {
                        Array.Copy(ZeroInt32, 0, rb, rbOffset, ZeroInt32.Length);
                    } else {
                        Array.Copy(BitConverter.GetBytes(len), 0, rb, rbOffset, sizeof(int));
                    }
                    rbOffset += sizeof(int);
                    E.GetBytes(s, 0, s.Length, rb, rbOffset);
                    rbOffset += len;
                }

                if(rbOffset > 0) {
                    destination.Write(rb, 0, rbOffset);
                }
                
            } finally {
                BytePool.Return(rb);
            }
        }

        public static int Decode(Stream source, Span<string> data, Thrift.SchemaElement tse) {
            int remLength = (int)(source.Length - source.Position);

            if(remLength == 0)
                return 0;

            byte[] bRaw = ArrayPool<byte>.Shared.Rent(remLength);
            Span<byte> raw = bRaw.AsSpan(0, remLength);
            try {
                Read(source, raw);

                int i = 0;
                for(int spanIdx = 0; spanIdx < raw.Length && i < data.Length; i++) {
                    int length = raw.Slice(spanIdx, 4).ReadInt32();
#if NETSTANDARD2_0
                    data[i] = E.GetString(raw.Slice(spanIdx + 4, length).ToArray());
#else
                    data[i] = E.GetString(raw.Slice(spanIdx + 4, length));
#endif
                    spanIdx = spanIdx + 4 + length;
                }
                return i;
            } finally {
                ArrayPool<byte>.Shared.Return(bRaw);
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