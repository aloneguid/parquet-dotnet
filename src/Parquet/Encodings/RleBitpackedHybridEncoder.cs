using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using Parquet.Collections;
using Parquet.Extensions;

namespace Parquet.Encodings {
    static class RleBitpackedHybridEncoder {

        private static readonly ArrayPool<byte> BytePool = ArrayPool<byte>.Shared;

        /// <summary>
        /// Writes to target stream without jumping around, therefore can be used in forward-only stream.
        /// Before writing actual data, writes out int32 value indicating total data binary length.
        /// </summary>
        public static void EncodeWithLength(Stream s, int bitWidth, Span<int> data) {
            //write data to a memory buffer, as we need data length to be written before the data
            using(var ms = new MemoryStream()) {
                //write actual data
                Encode(ms, data, bitWidth);

                //int32 - length of data
                s.WriteInt32((int)ms.Length);

                //actual data
                ms.Position = 0;
                ms.CopyTo(s); //warning! CopyTo performs .Flush internally
            }
        }

        public static void Encode(Stream s, Span<int> data, int bitWidth) {
            using(var list = new SpanBackedByteList()) {
                Encode(list, data, bitWidth);
                list.Write(s);
            }
        }

        /// <summary>
        /// Encodes input data
        /// </summary>
        public static void Encode(IList<byte> s, Span<int> data, int bitWidth) {
            //for simplicity, we're only going to write RLE, however bitpacking needs to be implemented as well

            const int maxCount = int.MaxValue >> 1;  //max count for an integer with one lost bit

            //chunk identical values and write
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == maxCount) {
                    WriteRle(s, chunkCount, lastValue, bitWidth);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRle(s, chunkCount, lastValue, bitWidth);
        }

        private static void WriteRle(IList<byte> s, int chunkCount, int value, int bitWidth) {
            int header = 0x0; // the last bit for RLE is 0
            header = chunkCount << 1;
            int byteWidth = (bitWidth + 7) / 8; //number of whole bytes for this bit width

            WriteUnsignedVarInt(s, header);
            WriteIntBytes(s, value, byteWidth);
        }

        private static void WriteIntBytes(IList<byte> s, int value, int byteWidth) {
#if NETSTANDARD2_0
            byte[] bytes = BitConverter.GetBytes(value);
#else
            Span<byte> bytes = stackalloc byte[sizeof(int)];
            BitConverter.TryWriteBytes(bytes, value);
#endif

            switch(byteWidth) {
                case 0:
                    break;
                case 1:
                    s.Add(bytes[0]);
                    break;
                case 2:
                    s.Add(bytes[0]);
                    s.Add(bytes[1]);
                    break;
                case 3:
                    s.Add(bytes[0]);
                    s.Add(bytes[1]);
                    s.Add(bytes[2]);
                    break;
                case 4:
                    s.Add(bytes[0]);
                    s.Add(bytes[1]);
                    s.Add(bytes[2]);
                    s.Add(bytes[3]);
                    //s.AddRange(dataBytes, 0, dataBytes.Length);
                    break;
                default:
                    throw new IOException($"encountered bit width ({byteWidth}) that requires more than 4 bytes.");
            }
        }

        private static void WriteUnsignedVarInt(IList<byte> s, int value) {
            while(value > 127) {
                byte b = (byte)((value & 0x7F) | 0x80);

                s.Add(b);

                value >>= 7;
            }

            s.Add((byte)value);
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
                int header = data.ReadUnsignedVarInt(ref dataOffset);
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
                BitPackedEncoder.Unpack8Values(rawSpan.Slice(byteIndex), dest.Slice(valueIndex), bitWidth);
                unpacked += 8;
            }

            //if(unpacked > maxItems)
            //    throw new InvalidOperationException($"{unpacked} > {maxItems}");

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

        private static int MaskForBits(int width) {
            return (1 << width) - 1;
        }
    }
}