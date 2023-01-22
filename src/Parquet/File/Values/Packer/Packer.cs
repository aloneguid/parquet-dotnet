using System;

namespace Parquet.File.Values.Packer {
    /// <summary>
    /// 
    /// </summary>
    public class Packer : IPacker {
        const int _maxBitWidth = 64;
        private readonly int _bitWidth;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bitWidth"></param>
        public Packer(int bitWidth) => _bitWidth = bitWidth;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="inBytes"></param>
        /// <param name="outData"></param>
        /// <param name="outPos"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void Unpack8Values(ReadOnlySpan<byte> inBytes, long[] outData, int outPos) {
            if(_bitWidth == 0) {
                return;
            }

            for(int valueIndex = 0; valueIndex < 8; ++valueIndex) {
                int startIndex = valueIndex * _bitWidth / 8;
                int endIndex = PaddedByteCountFromBits((valueIndex + 1) * _bitWidth);

                long value = 0;
                for(int byteIndex = startIndex; byteIndex < endIndex; byteIndex++) {
                    long baseValue = inBytes[byteIndex];

                    ShiftMask shiftMask = GetShift(_bitWidth, byteIndex, valueIndex);

                    switch(shiftMask.Shift) {
                        case < 0:
                            baseValue >>= -shiftMask.Shift;
                            break;
                        case > 0:
                            baseValue <<= shiftMask.Shift;
                            break;
                    }

                    baseValue &= shiftMask.Mask;

                    value |= baseValue;
                }

                outData[valueIndex + outPos] = value;
            }
        }

        private record ShiftMask(int Shift, long Mask) {
            public int Shift { get; } = Shift;
            public long Mask { get; } = Mask;
        }

        private static int PaddedByteCountFromBits(int bitLength) => (bitLength + 7) / 8;

        private ShiftMask GetShift(int bitWidth, int byteIndex, int valueIndex) {
            // relative positions of the start and end of the value to the start and end of the byte
            int valueStartBitIndex = (valueIndex * bitWidth) - (8 * (byteIndex));
            int valueEndBitIndex = ((valueIndex + 1) * bitWidth) - (8 * (byteIndex + 1));
            
            // boundaries of the current value that we want
            int valueStartBitWanted = bitWidth - 1 - (valueEndBitIndex > 0 ? valueEndBitIndex : 0);
            
            // boundaries of the current byte that will receive them
            int byteStartBitWanted = 7 - (valueEndBitIndex > 0 ? 0 : -valueEndBitIndex);
            int byteEndBitWanted = 7 - (valueStartBitIndex < 0 ? 8 : 7 - valueStartBitIndex);
            
            int shift = valueStartBitWanted - byteStartBitWanted;
            int widthWanted = Math.Max(0, byteStartBitWanted) - Math.Max(0, byteEndBitWanted) + 1;

            int maskWidth = widthWanted + Math.Max(0, shift);

            return new ShiftMask(shift, GenMaskForLong(maskWidth));
        }

        private static long GenMaskForLong(int bitWidth) {
            if(bitWidth >= _maxBitWidth) {
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
    }
}