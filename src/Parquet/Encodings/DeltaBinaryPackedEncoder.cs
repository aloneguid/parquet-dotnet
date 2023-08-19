using System;
using System.IO;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Meta;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BINARY_PACKED (https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5)
    /// fastparquet sample: https://github.com/dask/fastparquet/blob/c59e105537a8e7673fa30676dfb16d9fa5fb1cac/fastparquet/cencoding.pyx#L232
    /// golang sample: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingread.go#L270
    /// 
    /// Supported Types: INT32, INT64
    /// </summary>
    static partial class DeltaBinaryPackedEncoder {

        public static bool IsSupported(System.Type t) => t == typeof(int) || t == typeof(long);

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
            if(t == typeof(int[])) {
                EncodeInt(((int[])data).AsSpan(offset, count), destination, 1024, 32);
                if(stats != null)
                    ParquetPlainEncoder.FillStats((int[])data, stats);
            } else if(t == typeof(long[])) {
                EncodeLong(((long[])data).AsSpan(offset, count), destination, 1024, 32);
                if(stats != null)
                    ParquetPlainEncoder.FillStats((long[])data, stats);
            } else {
                throw new NotSupportedException($"only {Parquet.Meta.Type.INT32} and {Parquet.Meta.Type.INT64} are supported in {Encoding.DELTA_BINARY_PACKED} but type passed is {t}");
            }
        }

        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount, out int consumedBytes) {

            if(s.Length == 0 && valueCount == 0) {
                consumedBytes = 0;
                return 0;
            }

            System.Type? elementType = dest.GetType().GetElementType();
            if(elementType != null) {
                if(elementType == typeof(long)) {
                    Span<long> span = ((long[])dest).AsSpan(destOffset);
                    return DecodeLong(s, span, out consumedBytes);
                } else if(elementType == typeof(int)) {
                    Span<int> span = ((int[])dest).AsSpan(destOffset);
                    return DecodeInt(s, span, out consumedBytes);
                } else {
                    throw new NotSupportedException($"only {Parquet.Meta.Type.INT32} and {Parquet.Meta.Type.INT64} are supported in {Encoding.DELTA_BINARY_PACKED} but element type passed is {elementType}");
                }
            }

            throw new NotSupportedException($"element type {elementType} is not supported");
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
    }
}
