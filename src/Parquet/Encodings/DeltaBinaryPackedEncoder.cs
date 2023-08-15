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
    static partial class DeltaBinaryPackedEncoder {

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

        private static void WriteUnsignedVarInt(Stream destination, int value) {
            WriteUnsignedVarInt(destination, (uint)value);
        }

        private static void WriteZigZagVarLong(Stream destination, long value) {
            ulong zigZagEncoded = value.GetZigZagEncoded();
            WriteUnsignedVarLong(destination, zigZagEncoded);
        }
        private static void WriteUnsignedVarLong(Stream stream, ulong value) {
            byte[] buffer = new byte[10];
            int index = 0;

            while(value >= 0x80) {
                buffer[index++] = (byte)(value | 0x80);
                value >>= 7;
            }

            buffer[index++] = (byte)value;
            stream.Write(buffer, 0, index);
        }

        private static void WriteUnsignedVarInt(Stream stream, uint value) {
            byte[] buffer = new byte[5];
            int index = 0;

            while(value >= 0x80) {
                buffer[index++] = (byte)(value | 0x80);
                value >>= 7;
            }

            buffer[index++] = (byte)value;
            stream.Write(buffer, 0, index);
        }
    }
}