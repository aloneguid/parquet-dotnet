using System;
using System.Collections.Generic;

namespace Parquet.Encodings {
    static partial class BitPackedEncoder {

        /// <summary>
        /// Bit-packing used in RLE. Unlike pack, checks for boundaries.
        /// </summary>
        /// <returns>Number of bytes written</returns>
        public static int Encode8Values(Span<int> src, Span<byte> dest, int bitWidth) {

            bool needsTempBuffer = dest.Length < bitWidth;
            int written = bitWidth;

            Span<int> src1;
            Span<byte> dest1;
            if(needsTempBuffer) {
                src1 = new int[8];
                src.CopyTo(src1);
                dest1 = new byte[bitWidth];
                written = dest.Length;
            } else {
                src1 = src;
                dest1 = dest;
            }

            Pack8Values(src1, dest1, bitWidth);

            if(needsTempBuffer) {
                for(int i = 0; i < bitWidth && i < dest.Length; i++) {
                    dest[i] = dest1[i];
                }
            }

            return written;
        }

        /// <summary>
        /// Unlike unpack, checks for boundaries
        /// </summary>
        /// <param name="src"></param>
        /// <param name="bitWidth"></param>
        /// <param name="dest"></param>
        /// <returns>Number of values encoded</returns>
        public static int Decode8Values(Span<byte> src, Span<int> dest, int bitWidth) {

            // we always need at least bitWidth bytes available to decode 8 values
            bool needsTempFuffer = src.Length < bitWidth;
            int decoded = 8;

            Span<byte> src1;
            Span<int> dest1;
            if (needsTempFuffer) {
                src1 = new byte[bitWidth];
                src.CopyTo(src1);
                dest1 = new int[8];
                decoded = src.Length * 8 / bitWidth;
            } else {
                src1 = src;
                dest1 = dest;
            }

            Unpack8Values(src1, dest1, bitWidth);

            if(needsTempFuffer) {
                for(int i = 0; i < decoded; i++) {
                    dest[i] = dest1[i];
                }
            }

            return decoded;
        }

        /// <summary>
        /// Encode in legacy format (BIT_PACKED = 4).
        /// https://github.com/apache/parquet-format/blob/master/Encodings.md#bit-packed-deprecated-bit_packed--4
        /// </summary>
        public static void LegacyEncode(IEnumerable<int> values, int bitWidth, IList<byte> dest) {
            byte b = 0;
            int bi = 0;

            foreach(int v in values) {
                int done = 0;
                int todo = bitWidth;
                while(todo > 0) {

                    int cando = Math.Min(8 - bi, todo);

                    int mask1 = MaskForBits(bitWidth);
                    int mask2 = MaskForBits(bitWidth - done);
                    int mask = mask1 & mask2;

                    int mv1 = v & mask;
                    int mv2 = mv1 >> (todo - cando);

                    int mv3 = mv2 << (8 - bi - cando);
                    b ^= (byte)mv3;

                    todo -= cando;
                    bi += cando;
                    done += cando;

                    // finishing
                    if(bi == 8) {
                        dest.Add(b);
                        bi = 0;
                        b = 0;
                    }
                }
            }

            if(bi > 0) {
                dest.Add(b);
            }
        }

        public static int LegacyDecode(Span<byte> data, int bitWidth, int[] dest, int destOffset, int maxCount) {

            // this is genius: https://github.com/dask/fastparquet/blob/c59e105537a8e7673fa30676dfb16d9fa5fb1cac/fastparquet/cencoding.pyx#L216

            int dataIdx = 0;
            uint v = 0;
            int stop = -bitWidth;
            uint mask = 0xffffffff >> ((sizeof(int) * 8) - bitWidth);

            while(maxCount > 0) {
                if(stop < 0) {
                    v = ((v & 0x00ffffff) << 8) | data[dataIdx++];
                    stop += 8;
                } else {
                    dest[destOffset++] = (int)((v >> stop) & mask);
                    stop -= bitWidth;
                    maxCount -= 1;
                }
            }

            return dataIdx;
        }

        /// <summary>
        /// Generate a mask to grab `i` bits from an int value.
        /// </summary>
        private static int MaskForBits(int width) {
            return (1 << width) - 1;
        }
    }
}
