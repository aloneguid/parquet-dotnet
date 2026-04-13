using System;
using System.Buffers;
using System.Text;

namespace Parquet.Encodings;

/// <summary>
/// DELTA_LENGTH_BYTE_ARRAY = 6
/// Supported Types: BYTE_ARRAY
/// see https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6
/// </summary>
static class DeltaLengthByteArrayEncoder {

    private static readonly Encoding E = Encoding.UTF8;

    private static int MakeStrings(Span<byte> s, Span<int> lengths, Span<ReadOnlyMemory<char>> dest) {
        Decoder decoder = System.Text.Encoding.UTF8.GetDecoder();
        int made = 0;
        foreach(int len in lengths) {
            Span<byte> elSpan = s.Slice(0, len);
            char[] charBuffer = new char[System.Text.Encoding.UTF8.GetCharCount(elSpan)];
            decoder.GetChars(elSpan, charBuffer, true);
            dest[made] = charBuffer;
            s = s.Slice(len);
            made++;
        }
        return made;
    }

    private static int MakeBytes(Span<byte> s, Span<int> lengths, Span<ReadOnlyMemory<byte>> dest) {
        int made = 0;
        foreach(int len in lengths) {
            Span<byte> elSpan = s.Slice(0, len);
            dest[made] = elSpan.ToArray();
            s = s.Slice(len);
            made++;
        }
        return made;
    }

    public static int Decode<T>(Span<byte> s, Span<T> dest, int valueCount) where T : struct {

        int[] lengths = ArrayPool<int>.Shared.Rent(valueCount);
        try {
            int actualCount = DeltaBinaryPackedEncoder.Decode(s, lengths, 0, valueCount, out int lengthConsumedBytes);
            s = s.Slice(lengthConsumedBytes);

            if(typeof(T) == typeof(ReadOnlyMemory<char>)) {
                return MakeStrings(s, lengths.AsSpan(0, valueCount), dest.AsSpan<T, ReadOnlyMemory<char>>());
            } else if(typeof(T) == typeof(ReadOnlyMemory<byte>)) {
                return MakeBytes(s, lengths.AsSpan(0, valueCount), dest.AsSpan<T, ReadOnlyMemory<byte>>());
            } else {
                throw new NotSupportedException($"unsupported type {typeof(T)}, delta length byte arrays only can do strings and byte arrays");
            }

        } finally {
            ArrayPool<int>.Shared.Return(lengths);
        }
    }
}
