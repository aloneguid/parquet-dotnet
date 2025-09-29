using System;
using System.IO;
using System.Runtime.CompilerServices;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Meta;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BINARY_PACKED (https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5)
    /// fastparquet sample: https://github.com/dask/fastparquet/blob/c59e105537a8e7673fa30676dfb16d9fa5fb1cac/fastparquet/cencoding.pyx#L232
    /// golang sample: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingread.go#L270
    ///
    /// Supported Types: short, ushort, int, uint, long, ulong
    /// </summary>
    static partial class DeltaBinaryPackedEncoder {

        public static bool IsSupported(System.Type t) =>
            t == typeof(int) || t == typeof(long) ||           // native types
            t == typeof(short) || t == typeof(ushort) ||       // int32 compatible
            t == typeof(uint) || t == typeof(ulong);           // int64 compatible

        /// <summary>
        /// Determines whether the specified data can be encoded using delta binary packed encoding.
        /// For ulong arrays, checks if all values are within the range of long.MaxValue.
        /// </summary>
        /// <param name="data">The input array to check</param>
        /// <param name="offset">Starting offset in the array</param>
        /// <param name="count">Number of elements to check</param>
        /// <returns>True if the data can be delta encoded, false otherwise</returns>
        public static bool CanEncode(Array data, int offset, int count) {
            if (count == 0) return true;

            // Fast path for ulong overflow check
            if (data.GetType() == typeof(ulong[])) {
                return CanEncodeULongArray((ulong[])data, offset, count);
            }

            return IsSupported(data.GetType().GetElementType() ?? data.GetType());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CanEncodeULongArray(ulong[] data, int offset, int count) {
            const ulong maxValue = (ulong)long.MaxValue;
            int end = offset + count;

            for (int i = offset; i < end; i++) {
                if (data[i] > maxValue) {
                    return false;
                }
            }
            return true;
        }




        /// <summary>
        /// Encodes the provided data using a delta encoding scheme and writes it to the given destination stream.
        /// Optionally, collects statistics about the encoded data if the 'stats' parameter is provided.
        /// </summary>
        /// <param name="data">The input array to be encoded.</param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <param name="destination">The stream where the encoded data will be written.</param>
        /// <param name="stats">Optional parameter to collect statistics about the encoded data (can be null).</param>
        /// <exception cref="NotSupportedException"></exception>
        public static void Encode(Array data, int offset, int count, Stream destination, DataColumnStatistics? stats = null) {
            System.Type t = data.GetType();

            // Native types - no conversion needed
            if (t == typeof(int[])) {
                EncodeInt(((int[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((int[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(long[])) {
                EncodeLong(((long[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((long[])data).AsSpan(offset, count), stats);
            }
            // Direct encoding for all supported types
            else if (t == typeof(short[])) {
                EncodeShort(((short[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((short[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(ushort[])) {
                EncodeUshort(((ushort[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((ushort[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(uint[])) {
                EncodeUint(((uint[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((uint[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(ulong[])) {
                if (!CanEncodeULongArray((ulong[])data, offset, count)) {
                    throw new NotSupportedException($"ulong values exceed long.MaxValue range and cannot be encoded with {Encoding.DELTA_BINARY_PACKED}. Use plain encoding instead.");
                }
                EncodeUlong(((ulong[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((ulong[])data).AsSpan(offset, count), stats);
            }
            else {
                throw new NotSupportedException($"type {t} is not supported in {Encoding.DELTA_BINARY_PACKED}");
            }
        }




        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount, out int consumedBytes) {
            if (s.Length == 0 && valueCount == 0) {
                consumedBytes = 0;
                return 0;
            }

            System.Type? elementType = dest.GetType().GetElementType();
            if (elementType == null) {
                throw new NotSupportedException($"element type {elementType} is not supported");
            }

            // Native types - no conversion needed
            if (elementType == typeof(long)) {
                return DecodeLong(s, ((long[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(int)) {
                return DecodeInt(s, ((int[])dest).AsSpan(destOffset), out consumedBytes);
            }

            // Direct decoding for all supported types
            if (elementType == typeof(short)) {
                return DecodeShort(s, ((short[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(ushort)) {
                return DecodeUshort(s, ((ushort[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(uint)) {
                return DecodeUint(s, ((uint[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(ulong)) {
                return DecodeUlong(s, ((ulong[])dest).AsSpan(destOffset), out consumedBytes);
            }

            throw new NotSupportedException($"element type {elementType} is not supported in {Encoding.DELTA_BINARY_PACKED}");
        }


        //this extension method calculates the position of the most significant bit that is set to 1 
        static int CalculateBitWidth(this Span<int> span) {
            int mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 32 - mask.NumberOfLeadingZerosInt();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<long> span) {
            long mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 64 - mask.NumberOfLeadingZerosLong();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<short> span) {
            int mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= (int)span[i];
            }
            return 32 - mask.NumberOfLeadingZerosInt();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<ushort> span) {
            int mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 32 - mask.NumberOfLeadingZerosInt();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<uint> span) {
            long mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 64 - mask.NumberOfLeadingZerosLong();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<ulong> span) {
            ulong mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 64 - ((long)mask).NumberOfLeadingZerosLong();
        }
    }
}
