

namespace Parquet.Encodings {
    using System;

    static partial class BitPackedEncoder {

#region [ int ]
        struct ShiftMaskInt {
            public int shift;
            public int mask;
        }

        const int MaxBitWidthInt = sizeof(int) * 8;

        private static int GenMaskInt(int bitWidth) {
            if(bitWidth >= MaxBitWidthInt) {
                // -1 is always ones (11111...1111). It covers all it can possibly can.
                return -1;
            }

            int mask = 0;
            for(int i = 0; i < bitWidth; i++) {
                mask <<= 1;
                mask |= 1;
            }

            return mask;
        }

        private static ShiftMaskInt GetShiftInt(int bitWidth, int byteIndex, int valueIndex, bool msbFirst) {
            // relative positions of the start and end of the value to the start and end of the byte
            int valueStartBitIndex = (valueIndex * bitWidth) - (8 * byteIndex);
            int valueEndBitIndex = ((valueIndex + 1) * bitWidth) - (8 * (byteIndex + 1));

            // boundaries of the current value that we want
            int valueStartBitWanted;
            int valueEndBitWanted;
            // boundaries of the current byte that will receive them
            int byteStartBitWanted;
            int byteEndBitWanted;

            int shift;
            int widthWanted;

            if (msbFirst) {
                valueStartBitWanted = valueStartBitIndex < 0 ? bitWidth - 1 + valueStartBitIndex : bitWidth - 1;
                valueEndBitWanted = valueEndBitIndex > 0 ? valueEndBitIndex : 0;
                byteStartBitWanted = valueStartBitIndex < 0 ? 8 : 7 - valueStartBitIndex;
                byteEndBitWanted = valueEndBitIndex > 0 ? 0 : -valueEndBitIndex;
                shift = valueEndBitWanted - byteEndBitWanted;
                widthWanted = Math.Min(7, byteStartBitWanted) - Math.Min(7, byteEndBitWanted) + 1;
            } else {
                valueStartBitWanted = bitWidth - 1 - (valueEndBitIndex > 0 ? valueEndBitIndex : 0);
                valueEndBitWanted = bitWidth - 1 - (valueStartBitIndex < 0 ? bitWidth - 1 + valueStartBitIndex : bitWidth - 1);
                byteStartBitWanted = 7 - (valueEndBitIndex > 0 ? 0 : -valueEndBitIndex);
                byteEndBitWanted = 7 - (valueStartBitIndex < 0 ? 8 : 7 - valueStartBitIndex);
                shift = valueStartBitWanted - byteStartBitWanted;
                widthWanted = Math.Max(0, byteStartBitWanted) - Math.Max(0, byteEndBitWanted) + 1;
            }

            int maskWidth = widthWanted + Math.Max(0, shift);

            return new ShiftMaskInt{ shift = shift, mask = GenMaskInt(maskWidth)};
        }

        private static void Unpack8Values(Span<byte> src, int bitWidth, Span<int> dest, bool msbFirst) {
            if(bitWidth == 0)
                return;

            int destIdx = 0;

            for(int valueIndex = 0; valueIndex < 8; ++valueIndex) {
                int startIndex = valueIndex * bitWidth / 8;
                int endIndex = PaddedByteCountFromBits((valueIndex + 1) * bitWidth);

                int value = 0;
                for(int byteIndex = startIndex; byteIndex < endIndex && byteIndex < src.Length; byteIndex++) {
                    int baseValue = src[byteIndex];

                    ShiftMaskInt shiftMask = GetShiftInt(bitWidth, byteIndex, valueIndex, msbFirst);

                    if(shiftMask.shift < 0) {
                        baseValue >>= -shiftMask.shift;
                    } else if(shiftMask.shift > 0) {
                        baseValue <<= shiftMask.shift;
                    }

                    baseValue &= shiftMask.mask;

                    value |= baseValue;
                }

                dest[destIdx++] = value;
            }
        }


#endregion

#region [ long ]
        struct ShiftMaskLong {
            public int shift;
            public long mask;
        }

        const int MaxBitWidthLong = sizeof(long) * 8;

        private static long GenMaskLong(int bitWidth) {
            if(bitWidth >= MaxBitWidthLong) {
                // -1 is always ones (11111...1111). It covers all it can possibly can.
                return -1;
            }

            long mask = 0;
            for(int i = 0; i < bitWidth; i++) {
                mask <<= 1;
                mask |= 1;
            }

            return mask;
        }

        private static ShiftMaskLong GetShiftLong(int bitWidth, int byteIndex, int valueIndex, bool msbFirst) {
            // relative positions of the start and end of the value to the start and end of the byte
            int valueStartBitIndex = (valueIndex * bitWidth) - (8 * byteIndex);
            int valueEndBitIndex = ((valueIndex + 1) * bitWidth) - (8 * (byteIndex + 1));

            // boundaries of the current value that we want
            int valueStartBitWanted;
            int valueEndBitWanted;
            // boundaries of the current byte that will receive them
            int byteStartBitWanted;
            int byteEndBitWanted;

            int shift;
            int widthWanted;

            if (msbFirst) {
                valueStartBitWanted = valueStartBitIndex < 0 ? bitWidth - 1 + valueStartBitIndex : bitWidth - 1;
                valueEndBitWanted = valueEndBitIndex > 0 ? valueEndBitIndex : 0;
                byteStartBitWanted = valueStartBitIndex < 0 ? 8 : 7 - valueStartBitIndex;
                byteEndBitWanted = valueEndBitIndex > 0 ? 0 : -valueEndBitIndex;
                shift = valueEndBitWanted - byteEndBitWanted;
                widthWanted = Math.Min(7, byteStartBitWanted) - Math.Min(7, byteEndBitWanted) + 1;
            } else {
                valueStartBitWanted = bitWidth - 1 - (valueEndBitIndex > 0 ? valueEndBitIndex : 0);
                valueEndBitWanted = bitWidth - 1 - (valueStartBitIndex < 0 ? bitWidth - 1 + valueStartBitIndex : bitWidth - 1);
                byteStartBitWanted = 7 - (valueEndBitIndex > 0 ? 0 : -valueEndBitIndex);
                byteEndBitWanted = 7 - (valueStartBitIndex < 0 ? 8 : 7 - valueStartBitIndex);
                shift = valueStartBitWanted - byteStartBitWanted;
                widthWanted = Math.Max(0, byteStartBitWanted) - Math.Max(0, byteEndBitWanted) + 1;
            }

            int maskWidth = widthWanted + Math.Max(0, shift);

            return new ShiftMaskLong{ shift = shift, mask = GenMaskLong(maskWidth)};
        }

        private static void Unpack8Values(Span<byte> src, int bitWidth, Span<long> dest, bool msbFirst) {
            if(bitWidth == 0)
                return;

            int destIdx = 0;

            for(int valueIndex = 0; valueIndex < 8; ++valueIndex) {
                int startIndex = valueIndex * bitWidth / 8;
                int endIndex = PaddedByteCountFromBits((valueIndex + 1) * bitWidth);

                long value = 0;
                for(int byteIndex = startIndex; byteIndex < endIndex && byteIndex < src.Length; byteIndex++) {
                    long baseValue = src[byteIndex];

                    ShiftMaskLong shiftMask = GetShiftLong(bitWidth, byteIndex, valueIndex, msbFirst);

                    if(shiftMask.shift < 0) {
                        baseValue >>= -shiftMask.shift;
                    } else if(shiftMask.shift > 0) {
                        baseValue <<= shiftMask.shift;
                    }

                    baseValue &= shiftMask.mask;

                    value |= baseValue;
                }

                dest[destIdx++] = value;
            }
        }


#endregion

    }
}