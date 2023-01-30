using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Collections;
using Parquet.Extensions;

namespace Parquet.Data {
    static class RleEncoder {
        /// <summary>
        /// Writes to target stream without jumping around, therefore can be used in forward-only stream.
        /// Before writing actual data, writes out int32 value indicating total data binary length.
        /// </summary>
        public static void EncodeWithLength(Stream s, int bitWidth, int[] data, int count) {
            //write data to a memory buffer, as we need data length to be written before the data
            using(var ms = new MemoryStream()) {
                //write actual data
                Encode(ms, data, count, bitWidth);

                //int32 - length of data
                s.WriteInt32((int)ms.Length);

                //actual data
                ms.Position = 0;
                ms.CopyTo(s); //warning! CopyTo performs .Flush internally
            }
        }

        public static void Encode(Stream s, int[] data, int count, int bitWidth) {
            using(var list = new SpanBackedByteList()) {
                Encode(list, data, count, bitWidth);
                list.Write(s);
            }
        }

        /// <summary>
        /// Encodes input data
        /// </summary>
        public static void Encode(IList<byte> s, int[] data, int count, int bitWidth) {
            //for simplicity, we're only going to write RLE, however bitpacking needs to be implemented as well

            const int maxCount = int.MaxValue >> 1;  //max count for an integer with one lost bit

            //chunk identical values and write
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < count; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                }
                else {
                    if(item != lastValue || chunkCount == maxCount) {
                        WriteRle(s, chunkCount, lastValue, bitWidth);

                        chunkCount = 1;
                        lastValue = item;
                    }
                    else {
                        chunkCount += 1;
                    }
                }
            }

            if(chunkCount > 0) {
                WriteRle(s, chunkCount, lastValue, bitWidth);
            }
        }

        /// <summary>
        /// Decodes data
        /// </summary>
        public static int Decode(Stream s, int[] dest, int destOffset, int bitWidth, int maxReadCount) {
            int length = GetRemainingLength(s);
            return Decode(s, bitWidth, length, dest, destOffset, maxReadCount);
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

        public static int Decode(Stream s, int bitWidth, int length, int[] dest, int offset, int pageSize) {
            if(length == 0)
                length = s.ReadInt32();

            long start = s.Position;
            int startOffset = offset;
            while((s.Position - start < length)) {
                int header = ReadUnsignedVarInt(s);
                bool isRle = (header & 1) == 0;

                if(isRle) {
                    offset += ReadRle(header, s, bitWidth, dest, offset, pageSize - (offset - startOffset));
                }
                else {
                    offset += ReadBitpacked(header, s, bitWidth, dest, offset, pageSize - (offset - startOffset));
                }
            }

            return offset - startOffset;
        }


        /// <summary>
        /// Read run-length encoded run from the given header and bit length.
        /// </summary>
        private static void ReadRle(int header, Stream s, int bitWidth, List<int> destination) {
            // The count is determined from the header and the width is used to grab the
            // value that's repeated. Yields the value repeated count times.

            int count = header >> 1;
            if(count == 0)
                return; //important not to continue reading as will result in data corruption in data page further
            int width = (bitWidth + 7) / 8; //round up to next byte
            byte[] data = new byte[width];
            s.Read(data, 0, width);
            int value = ReadIntOnBytes(data);
            destination.AddRange(Enumerable.Repeat(value, count));
        }

        private static int ReadRle(int header, Stream s, int bitWidth, int[] dest, int offset, int maxItems) {
            // The count is determined from the header and the width is used to grab the
            // value that's repeated. Yields the value repeated count times.

            int start = offset;
            int headerCount = header >> 1;
            if(headerCount == 0)
                return 0; // important not to continue reading as will result in data corruption in data page further
            int count = Math.Min(headerCount, maxItems); // make sure we remain within bounds
            int width = (bitWidth + 7) / 8; // round up to next byte
            byte[] data = new byte[width];
            s.Read(data, 0, width);
            int value = ReadIntOnBytes(data);

            for(int i = 0; i < count; i++) {
                dest[offset++] = value;
            }

            return offset - start;
        }

        private static int ReadBitpacked(int header, Stream s, int bitWidth, int[] dest, int offset, int maxItems) {
            int start = offset;
            int groupCount = header >> 1;
            int count = groupCount * 8;
            int byteCount = bitWidth * count / 8;
            //int byteCount2 = (int)Math.Ceiling(bitWidth * count / 8.0);

            byte[] rawBytes = s.ReadBytesExactly(byteCount, true);
            byteCount = rawBytes.Length;  //sometimes there will be less data available, typically on the last page

            int mask = MaskForBits(bitWidth);

            int i = 0;
            uint b = rawBytes[i];
            int total = byteCount * 8;
            int bwl = 8;
            int bwr = 0;
            while(total >= bitWidth && (offset - start) < maxItems) {
                if(bwr >= 8) {
                    bwr -= 8;
                    bwl -= 8;
                    b >>= 8;
                }
                else if(bwl - bwr >= bitWidth) {
                    int r = (int)((b >> bwr) & mask);
                    total -= bitWidth;
                    bwr += bitWidth;

                    dest[offset++] = r;
                }
                else if(i + 1 < byteCount) {
                    i += 1;
                    b |= (uint)(rawBytes[i] << bwl);
                    bwl += 8;
                }
            }

            return offset - start;
        }



        /// <summary>
        /// Read a value using the unsigned, variable int encoding.
        /// </summary>
        private static int ReadUnsignedVarInt(Stream s) {
            int result = 0;
            int shift = 0;

            while(true) {
                byte b = (byte)s.ReadByte();
                result |= ((b & 0x7F) << shift);
                if((b & 0x80) == 0)
                    break;
                shift += 7;
            }

            return result;
        }

        private static int ReadIntOnBytes(byte[] data) {
            switch(data.Length) {
                case 0:
                    return 0;
                case 1:
                    return (data[0]);
                case 2:
                    return (data[1] << 8) + data[0];
                case 3:
                    return (data[2] << 16) + (data[1] << 8) + data[0];
                case 4:
                    return BitConverter.ToInt32(data, 0);
                default:
                    throw new IOException($"encountered byte width ({data.Length}) that requires more than 4 bytes.");
            }
        }

        private static int MaskForBits(int width) {
            return (1 << width) - 1;
        }

        private static int GetRemainingLength(Stream s) {
            return (int)(s.Length - s.Position);
        }
    }


}
