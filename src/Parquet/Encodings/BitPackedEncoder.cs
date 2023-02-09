using System;
using System.Collections.Generic;

namespace Parquet.Encodings {
    static partial class BitPackedEncoder {

        private const int MaxBitWidth = 32;

        /// <summary>
        /// Bit-packing used in RLE
        /// </summary>
        public static void Encode(IEnumerable<int> values, int bitWidth, IList<byte> dest) {
            throw new NotImplementedException();
        }

        public static int Decode(Span<byte> src, int bitWidth, Span<int> dest) {
            throw new NotImplementedException();
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
