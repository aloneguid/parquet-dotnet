using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_LENGTH_BYTE_ARRAY = 6
    /// Supported Types: BYTE_ARRAY
    /// see https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6
    /// </summary>
    static class DeltaLengthByteArrayEncoder {

        private static readonly Encoding E = Encoding.UTF8;

        private static int MakeStrings(Span<byte> s, Span<int> lengths, string[] dest, int destOffset) {
            int made = 0;
            foreach(int len in lengths) {
#if NETSTANDARD2_0
                dest[destOffset++] = E.GetString(s.Slice(0, len).ToArray());
#else
                dest[destOffset++] = E.GetString(s.Slice(0, len));
#endif
                s = s.Slice(len);
                made++;
            }
            return made;
        }

        private static int MakeBytes(Span<byte> s, Span<int> lengths, byte[][] dest, int destOffset) {
            int made = 0;
            foreach(int len in lengths) {
                dest[destOffset++] = s.Slice(0, len).ToArray();
                s = s.Slice(len);
                made++;
            }
            return made;
        }

        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount) {

            int[] lengths = ArrayPool<int>.Shared.Rent(valueCount);
            try {
                int actualCount = DeltaBinaryPackedEncoder.Decode(s, lengths, 0, valueCount, out int lengthConsumedBytes);
                s = s.Slice(lengthConsumedBytes);

                Type? et = dest.GetType().GetElementType();
                if(et == typeof(string)) {
                    return MakeStrings(s, lengths.AsSpan(0, valueCount), (string[])dest, destOffset);
                } else if(et == typeof(byte[])) {
                    return MakeBytes(s, lengths.AsSpan(0, valueCount), (byte[][])dest, destOffset);
                } else {
                    throw new NotSupportedException($"unsupported type {et}, delta length byte arrays only can do strings and byte arrays");
                }

            } finally {
                ArrayPool<int>.Shared.Return(lengths);
            }
        }
    }
}
