using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Parquet.Extensions;

namespace Parquet.Encodings {
    static partial class RleBitpackedHybridEncoder {

        private static readonly ArrayPool<byte> BytePool = ArrayPool<byte>.Shared;
        private const int MaxValueCount = int.MaxValue >> 1;  //max count for an integer with one lost bit

        /// <summary>
        /// Writes to target stream without jumping around, therefore can be used in forward-only stream.
        /// Before writing actual data, writes out int32 value indicating total data binary length.
        /// </summary>
        public static void EncodeWithLength(Stream s, int bitWidth, Span<int> data) {

            int length = Encode(data, bitWidth, out byte[]? rentedBuffer);
            if(rentedBuffer == null)
                return;
            try {
                s.WriteInt32(length);
                s.Write(rentedBuffer, 0, length);
            } finally {
                BytePool.Return(rentedBuffer);
            }
        }

        public static void Encode(Stream dest, Span<int> data, int bitWidth) {
            int length = Encode(data, bitWidth, out byte[]? rentedBuffer);
            if(rentedBuffer == null)
                return;
            try {
                dest.Write(rentedBuffer, 0, length);
            } finally {
                BytePool.Return(rentedBuffer);
            }
        }

        public static int Encode(Span<int> data, int bitWidth, out byte[]? rentedBuffer) {

            //for simplicity, we're only going to write RLE, however bitpacking needs to be implemented as well

            int byteWidth = (bitWidth + 7) / 8; //number of whole bytes for this bit width
            if(byteWidth > 4)
                throw new IOException($"encountered bit width ({byteWidth}) that requires more than 4 bytes.");

            // rent necessary buffer of max length
            rentedBuffer = BytePool.Rent((byteWidth + 4) * data.Length);
            int consumed = 0;

            try {
                switch(byteWidth) {
                    case 0:
                        Encode0(rentedBuffer, ref consumed, data);
                        break;
                    case 1:
                        Encode1(rentedBuffer, ref consumed, data);
                        break;
                    case 2:
                        Encode2(rentedBuffer, ref consumed, data);
                        break;
                    case 3:
                        Encode3(rentedBuffer, ref consumed, data);
                        break;
                    case 4:
                        Encode4(rentedBuffer, ref consumed, data);
                        break;
                }
            } catch {
                BytePool.Return(rentedBuffer);
                rentedBuffer = null;
                throw;
            }

            return consumed;
        }

        private static void WriteRle0(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
        }

        private static void WriteRle1(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
        }

        private static void WriteRle2(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
            r[consumed++] = (byte)((value >> 8) & 0xFF);
        }

        private static void WriteRle3(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
            r[consumed++] = (byte)((value >> 8) & 0xFF);
            r[consumed++] = (byte)((value >> 16) & 0x00FF);
        }

        private static void WriteRle4(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
            r[consumed++] = (byte)((value >> 8) & 0xFF);
            r[consumed++] = (byte)((value >> 16) & 0x00FF);
            r[consumed++] = (byte)((value >> 32) & 0x0000FF);
        }

        public static int Decode(Span<byte> s,
            int bitWidth,
            int? spanLength,
            out int usedSpanLength,
            Span<int> dest, int pageSize) {

            int length;
            if(spanLength == null) {
                length = s.ReadInt32(0);
                s = s.Slice(sizeof(int), length);
                usedSpanLength = length + sizeof(int);
            } else {
                length = spanLength.Value;
                s = s.Slice(0, length);
                usedSpanLength = length;
            }

            return Decode(s, bitWidth, dest, pageSize);
        }

        /* from specs:
         * rle-bit-packed-hybrid: <length> <encoded-data>
         * length := length of the <encoded-data> in bytes stored as 4 bytes little endian
         * encoded-data := <run>*
         * run := <bit-packed-run> | <rle-run>  
         * bit-packed-run := <bit-packed-header> <bit-packed-values>  
         * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)  
         * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8  
         * bit-pack-count := (number of values in this run) / 8  
         * bit-packed-values := *see 1 below*  
         * rle-run := <rle-header> <repeated-value>  
         * rle-header := varint-encode( (number of times repeated) << 1)  
         * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
         */

        public static int Decode(Span<byte> data, int bitWidth, Span<int> dest, int pageSize) {
            int dataOffset = 0;

            int byteWidth = (bitWidth + 7) / 8; //round up to next byte
            int destOffset = 0;
            while(dataOffset < data.Length) {
                int header = (int)data.ULEB128Decode(ref dataOffset);
                //int header = (int)data.ULEB128Decode(ref dataOffset);
                bool isRle = (header & 1) == 0;

                if(isRle)
                    destOffset += ReadRle(header, data, ref dataOffset, byteWidth, dest.Slice(destOffset), pageSize - destOffset);
                else
                    destOffset += DecodeBitpacked(header, data, ref dataOffset, bitWidth, dest.Slice(destOffset), pageSize - destOffset);
            }

            return destOffset;
        }

        private static int ReadRle(int header, Span<byte> data, ref int offset, int byteWidth, Span<int> dest, int maxItems) {
            // The count is determined from the header and the width is used to grab the
            // value that's repeated. Yields the value repeated count times.

            int destOffset = 0;
            int headerCount = header >> 1;
            if(headerCount == 0)
                return 0; // important not to continue reading as will result in data corruption in data page further
            int count = Math.Min(headerCount, maxItems); // make sure we remain within bounds
            int value = ReadIntOnBytes(data.Slice(offset, byteWidth));
            offset += byteWidth;

            for(int i = 0; i < count; i++)
                dest[destOffset++] = value;

            return destOffset;
        }

        internal static int DecodeBitpacked(int header, Span<byte> data, ref int dataOffset,
            int bitWidth,
            Span<int> dest, int maxItems) {

            int unpacked = 0;
            int numGroups = header >> 1;

            if(numGroups == 0)
                return 0;

            int count = numGroups * 8;
            int bytesToRead = (int)Math.Ceiling(bitWidth * count / 8.0);
            bytesToRead = Math.Min(bytesToRead, data.Length - dataOffset);
            Span<byte> rawSpan = data.Slice(dataOffset, bytesToRead);
            dataOffset += bytesToRead;

            for(int valueIndex = 0, byteIndex = 0; valueIndex < count; valueIndex += 8, byteIndex += bitWidth) {
                unpacked += BitPackedEncoder.Decode8ValuesLE(rawSpan.Slice(byteIndex), dest.Slice(valueIndex), bitWidth);
            }

            return Math.Min(unpacked, maxItems);
        }

        private static int ReadIntOnBytes(Span<byte> data) {
            switch(data.Length) {
                case 0:
                    return 0;
                case 1:
                    return data[0];
                case 2:
                    return (data[1] << 8) + data[0];
                case 3:
                    return (data[2] << 16) + (data[1] << 8) + data[0];
                case 4:
                    return BitConverter.ToInt32(data.ToArray(), 0);
                default:
                    throw new IOException($"encountered byte width ({data.Length}) that requires more than 4 bytes.");
            }
        }
    }
}