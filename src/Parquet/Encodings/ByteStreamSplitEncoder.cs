using System;

namespace Parquet.Encodings {
    static class ByteStreamSplitEncoder {

        public static int DecodeByteStreamSplit(ReadOnlySpan<byte> s, Array dest, int destOffset, int valueCount) {
            System.Type? elementType = dest.GetType().GetElementType()!;
            int k;
            float[]? fDest = default;
            double[]? dDest = default;
            if(elementType == typeof(float)) {
                k = 4;
                fDest = (float[])dest;
            } else if(elementType == typeof(double)) {
                k = 8;
                dDest = (double[])dest;
            } else {
                throw new NotSupportedException(elementType.Name.ToString());
            }

            /*
             * Example: Original data is three 32-bit floats and for simplicity we look at their raw representation.
             *        Element 0      Element 1      Element 2
             * Bytes  AA BB CC DD    00 11 22 33    A3 B4 C5 D6
             * 
             * After applying the transformation, the data has the following representation:
             * Bytes  AA 00 A3 BB 11 B4 CC 22 C5 DD 33 D6
             */

#if NETSTANDARD2_1_OR_GREATER
            Span<byte> tmp = stackalloc byte[k];
            for(int i = 0; i < valueCount; i++) {
                for(int j = 0; j < k; j++) {
                    tmp[j] = s[(j * valueCount) + i];
                }
                if(k == 4) {
                    float value = BitConverter.ToSingle(tmp);
                    fDest![i] = value;
                } else {
                    double value = BitConverter.ToDouble(tmp);
                    dDest![i] = value;
                }
            }
#else
            byte[] tmp = ArrayPool<byte>.Shared.Rent(k);
            try {
                for(int i = 0; i < valueCount; i++) {
                    for(int j = 0; j < k; j++) {
                        tmp[j] = s[(j * valueCount) + i];
                    }
                    if(k == 4) {
                        float value = BitConverter.ToSingle(tmp, 0);
                        fDest![i] = value;
                    } else {
                        double value = BitConverter.ToDouble(tmp, 0);
                        dDest![i] = value;
                    }
                }
            } finally {
                ArrayPool<byte>.Shared.Return(tmp);
            }
#endif
            return valueCount;
        }
    }
}
