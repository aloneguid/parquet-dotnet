using System;
using System.Buffers;
using System.IO;
using Parquet.Data;
using Parquet.Meta;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BINARY_PACKED (https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5)
    /// fastparquet sample: https://github.com/dask/fastparquet/blob/c59e105537a8e7673fa30676dfb16d9fa5fb1cac/fastparquet/cencoding.pyx#L232
    /// golang sample: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingread.go#L270
    /// 
    /// Supported Types: INT32, INT64
    /// </summary>
    public static partial class DeltaBinaryPackedEncoder {

        private static readonly ArrayPool<byte> BytePool = ArrayPool<byte>.Shared;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <param name="dest"></param>
        /// <param name="destOffset"></param>
        /// <param name="valueCount"></param>
        /// <param name="consumedBytes"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount, out int consumedBytes) {
            System.Type? elementType = dest.GetType().GetElementType();
            if(elementType != null) {
                if(elementType == typeof(long)) {
                    Span<long> span = ((long[])dest).AsSpan(destOffset);
                    return Decode(s, span, out consumedBytes);
                } else if(elementType == typeof(int)) {
                    Span<int> span = ((int[])dest).AsSpan(destOffset);
                    return Decode(s, span, out consumedBytes);
                } else {
                    throw new NotSupportedException($"only {Parquet.Meta.Type.INT32} and {Parquet.Meta.Type.INT64} are supported in {Encoding.DELTA_BINARY_PACKED} but element type passed is {elementType}");
                }
            }

            throw new NotSupportedException($"element type {elementType} is not supported");
        }



        /// <summary>
        /// Encodes the provided data using a delta encoding scheme and writes it to the given destination stream.
        /// Optionally, collects statistics about the encoded data if the 'stats' parameter is provided.
        /// </summary>
        /// <param name="data">The input array to be encoded.</param>
        /// <param name="destination">The stream where the encoded data will be written.</param>
        /// <param name="stats">Optional parameter to collect statistics about the encoded data (can be null).</param>
        /// <exception cref="NotSupportedException"></exception>
        public static void Encode(Array data, Stream destination, DataColumnStatistics? stats = null) {
            System.Type t = data.GetType();
            if(t == typeof(int[])) {
                Encode((int[])data, destination);
                if(stats != null)
                    ParquetPlainEncoder.FillStats((int[])data, stats);
            } else if(t == typeof(long[])) {
                Encode((long[])data, destination);
                if(stats != null)
                    ParquetPlainEncoder.FillStats((long[])data, stats);
            } else {
                throw new NotSupportedException($"only {Parquet.Meta.Type.INT32} and {Parquet.Meta.Type.INT64} are supported in {Encoding.DELTA_BINARY_PACKED} but type passed is {t}");
            }
        }

        private static ulong ZigZagEncode(long num) {
            return (ulong)((num >> 63) ^ (num << 1));
        }

        private static uint ZigZagEncode(int num) {
            return (uint)((num >> 31) ^ (num << 1));
        }

        private static void WriteUnsignedVarInt(Stream destination, int value) {
            byte[] rentedBuffer = BytePool.Rent(4);
            int consumed = 0;
            try {
                WriteUnsignedVarInt(rentedBuffer, ref consumed, value);
                destination.Write(rentedBuffer, 0, consumed);
            } finally {
                BytePool.Return(rentedBuffer);
            }
        }
        private static void WriteUnsignedVarInt(Stream destination, ulong value) {
            byte[] rentedBuffer = BytePool.Rent(8);
            int consumed = 0;
            try {
                WriteUnsignedVarInt(rentedBuffer, ref consumed, value);
                destination.Write(rentedBuffer, 0, consumed);
            } finally {
                BytePool.Return(rentedBuffer);
            }
        }

        private static void WriteUnsignedVarInt(byte[] s, ref int consumed, int value) {
            while(value > 127) {
                byte b = (byte)((value & 0x7F) | 0x80);

                s[consumed++] = b;

                value >>= 7;
            }

            s[consumed++] = (byte)value;
        }

        private static byte[] WriteUnsignedVarInt(byte[] s, ref int consumed, ulong value) {
            consumed = 1;
            ulong numTmp = value;

            while(numTmp >= 128) {
                consumed++;
                numTmp >>= 7;
            }

            numTmp = value;
            for(int i = 0; i < consumed; i++) {
                s[i] = (byte)(numTmp | 0x80);
                numTmp >>= 7;
            }

            s[consumed - 1] &= 0x7F;
            return s;
        }
    }
}
