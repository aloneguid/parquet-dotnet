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

        private static void FlushBlock(Span<int> block, int minDelta,
            Stream destination,
            int miniblockCount, int miniblockSize) {

            // min delta can be flushed immediately
            destination.WriteUnsignedVarInt((int)minDelta.ZigZagEncode());

            // subtract minDelta from all values
            for(int i = 0; i < block.Length; i++) {
                block[i] = block[i] - minDelta;
            }

            // we need bit widths for each miniblock (after minDelta is applied)
            Span<byte> bitWidths = stackalloc byte[miniblockCount];

            for(int offset = 0, bwi = 0; offset < block.Length; offset += miniblockSize, bwi++) {
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 0)
                    break;

                int max = block.Slice(offset, count).Max();
                bitWidths[bwi] = (byte)max.GetBitWidth();
            }

            // write bit widths
#if NETSTANDARD2_0
            destination.Write(bitWidths.ToArray(), 0, miniblockCount);
#else
            destination.Write(bitWidths);
#endif

            // each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
            Span<int> raw8 = stackalloc int[8];
            for(int i = 0; i < miniblockCount; i++) {
                int offset = i * miniblockSize;
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 1)
                    break;
                Span<int> miniblockData = block.Slice(offset, count);
                // write values in 8
                int bitWidth = bitWidths[i];
                byte[] encoded8 = new byte[bitWidth];
                for(int iv = 0; iv < miniblockData.Length; iv += 8) {
                    int count8 = Math.Min(8, miniblockData.Length - iv);
                    miniblockData.Slice(iv, count8).CopyTo(raw8);
                    BitPackedEncoder.Pack8ValuesLE(raw8, encoded8, bitWidth);
                    destination.Write(encoded8, 0, bitWidth);
                }
            }
        }

        private static void EncodeInt32(ReadOnlySpan<int> data, Stream destination,
            int blockSize, int miniblockSize) {

            if(data.Length == 0)
                return;

            // header: <block size in values> <number of miniblocks in a block> <total value count> <first value>
            int miniblockCount = blockSize / miniblockSize;
            destination.WriteUnsignedVarInt(blockSize);
            destination.WriteUnsignedVarInt(miniblockCount);
            destination.WriteUnsignedVarInt(data.Length);
            destination.WriteUnsignedVarLong(data[0].ZigZagEncode());

            // each block: <min delta> <list of bitwidths of miniblocks> <miniblocks>
            Span<int> block = stackalloc int[blockSize];
            int blockCount = 0;
            int minDelta = 0;
            for(int i = 1; i < data.Length; i++) {

                // calculate delta element and minDelta
                int delta = data[i] - data[i - 1];
                if(blockCount == 0 || delta < minDelta) {
                    minDelta = delta;
                }
                block[blockCount++] = delta;

                // write block
                if(blockCount == blockSize) {
                    FlushBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
                    blockCount = 0;
                }
            }

            if(blockCount > 0) {
                FlushBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
            }
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
                EncodeInt32((int[])data, destination, 1024, 32);
                if(stats != null)
                    ParquetPlainEncoder.FillStats((int[])data, stats);
            } else if(t == typeof(long[])) {
                throw new NotImplementedException();
            } else {
                throw new NotSupportedException($"only {Parquet.Meta.Type.INT32} and {Parquet.Meta.Type.INT64} are supported in {Encoding.DELTA_BINARY_PACKED} but type passed is {t}");
            }
        }

        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount, out int consumedBytes) {
            System.Type? elementType = dest.GetType().GetElementType();
            if(elementType != null) {
                if(elementType == typeof(long)) {
                    Span<long> span = ((long[])dest).AsSpan(destOffset);
                    return DecodeInternal(s, span, out consumedBytes);
                } else if(elementType == typeof(int)) {
                    Span<int> span = ((int[])dest).AsSpan(destOffset);
                    return DecodeInternal(s, span, out consumedBytes);
                } else {
                    throw new NotSupportedException($"only {Parquet.Meta.Type.INT32} and {Parquet.Meta.Type.INT64} are supported in {Encoding.DELTA_BINARY_PACKED} but element type passed is {elementType}");
                }
            }

            throw new NotSupportedException($"element type {elementType} is not supported");
        }
    }
}
