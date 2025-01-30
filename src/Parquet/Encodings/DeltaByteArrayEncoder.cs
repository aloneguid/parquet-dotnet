using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BYTE_ARRAY = 7, aka "Delta Strings"
    /// Supported Types: BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
    /// see https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7
    /// </summary>
    /// <remarks>
    /// This encoding depends on DELTA_BINARY_PACKED and DELTA_LENGTH_BYTE_ARRAY
    /// </remarks>
    static class DeltaByteArrayEncoder {

        private static void MakeStrings(int[] prefixLenghs, string[] dest, int destOffset, int count) {
            if(count == 0) return;
            string v = dest[destOffset];
            for(int i = 1, di = destOffset + 1; i < count; i++, di++) { 
                string vNext = v.Substring(0, prefixLenghs[i]) + dest[di];
                v = vNext;
                dest[di] = v;
            }
        }

        private static void MakeBytes(int[] prefixLenghs, byte[][] dest, int destOffset, int count) {
            if(count == 0) return;
            byte[] v = dest[destOffset];
            for(int i = 1, di = destOffset + 1; i < count; i++, di++) {
                int pl = prefixLenghs[i];
                byte[] vNext = new byte[pl + dest[di].Length];
                Buffer.BlockCopy(v, 0, vNext, 0, pl);
                Buffer.BlockCopy(dest[di], 0, vNext, pl, dest[di].Length);
                v = vNext;
                dest[di] = v;
            }
        }

        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount) {

            // sequence of delta-encoded prefix lengths (DELTA_BINARY_PACKED)

            int[] prefixLenghs = ArrayPool<int>.Shared.Rent(valueCount);
            try {
                // prefix lengths encoded as DELTA_BINARY_PACKED
                int prefixLengthsCount = DeltaBinaryPackedEncoder.Decode(s, prefixLenghs, 0, valueCount, out int consumedBytes);

                // the suffixes encoded as delta length byte arrays (DELTA_LENGTH_BYTE_ARRAY).
                int suffixesCount = DeltaLengthByteArrayEncoder.Decode(s.Slice(consumedBytes), dest, destOffset, valueCount);

                Type? et = dest.GetType().GetElementType();
                if(et == typeof(string)) {
                    MakeStrings(prefixLenghs, (string[])dest, destOffset, valueCount);
                } else if(et == typeof(byte[])) {
                    MakeBytes(prefixLenghs, (byte[][])dest, destOffset, valueCount);
                } else {
                    throw new NotSupportedException($"unsupported type {et}, delta byte arrays only can do strings and byte arrays");
                }

                return valueCount;
            }
            finally {
                ArrayPool<int>.Shared.Return(prefixLenghs);
            }
        }
    }
}
