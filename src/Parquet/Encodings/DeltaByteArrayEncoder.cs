using System;
using System.Buffers;

namespace Parquet.Encodings;

/// <summary>
/// DELTA_BYTE_ARRAY = 7, aka "Delta Strings" Supported Types: BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY see
/// https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7
/// </summary>
/// <remarks>
/// This encoding depends on DELTA_BINARY_PACKED and DELTA_LENGTH_BYTE_ARRAY
/// </remarks>
static class DeltaByteArrayEncoder {

    private static void MakeStrings(int[] prefixLenghs, Span<ReadOnlyMemory<char>> dest, int count) {
        if(count == 0)
            return;
        ReadOnlyMemory<char> v = dest[0];
        for(int i = 1, di = 1; i < count; i++, di++) {
            ReadOnlyMemory<char> prefix = v.Slice(0, prefixLenghs[i]);
            ReadOnlyMemory<char> suffix = dest[di];

            // create new string by concatenating prefix and suffix, and assign it back to dest[di]
            Memory<char> vNext = new char[prefix.Length + suffix.Length];
            prefix.CopyTo(vNext);
            suffix.CopyTo(vNext.Slice(prefix.Length));

            ReadOnlyMemory<char> vNextReadOnly = vNext;
            v = vNextReadOnly;
            dest[di] = v;
        }
    }

    private static void MakeBytes(int[] prefixLenghs, Span<ReadOnlyMemory<byte>> dest, int count) {
        if(count == 0)
            return;
        ReadOnlyMemory<byte> v = dest[0];
        for(int i = 1, di = 1; i < count; i++, di++) {
            ReadOnlyMemory<byte> prefix = v.Slice(0, prefixLenghs[i]);
            ReadOnlyMemory<byte> suffix = dest[di];

            // create new byte array by concatenating prefix and suffix, and assign it back to dest[di]
            Memory<byte> vNext = new byte[prefix.Length + suffix.Length];
            prefix.CopyTo(vNext);
            suffix.CopyTo(vNext.Slice(prefix.Length));

            ReadOnlyMemory<byte> vNextReadOnly = vNext;
            v = vNextReadOnly;
            dest[di] = v;
        }
    }

    public static int Decode<T>(Span<byte> s, Span<T> dest, int valueCount) where T : struct  {

        // sequence of delta-encoded prefix lengths (DELTA_BINARY_PACKED)

        int[] prefixLenghs = ArrayPool<int>.Shared.Rent(valueCount);
        try {
            // prefix lengths encoded as DELTA_BINARY_PACKED
            int prefixLengthsCount = DeltaBinaryPackedEncoder.Decode(s, prefixLenghs, 0, valueCount, out int consumedBytes);

            // the suffixes encoded as delta length byte arrays (DELTA_LENGTH_BYTE_ARRAY).
            int suffixesCount = DeltaLengthByteArrayEncoder.Decode(s.Slice(consumedBytes), dest, valueCount);

            if(typeof(T) == typeof(ReadOnlyMemory<char>)) {
                MakeStrings(prefixLenghs, dest.AsSpan<T, ReadOnlyMemory<char>>(), valueCount);
            } else if(typeof(T) == typeof(ReadOnlyMemory<byte>)) {
                MakeBytes(prefixLenghs, dest.AsSpan<T, ReadOnlyMemory<byte>>(), valueCount);
            } else {
                throw new NotSupportedException($"unsupported type {typeof(T)}, delta byte arrays only can do strings and byte arrays");
            }

            return valueCount;
        } finally {
            ArrayPool<int>.Shared.Return(prefixLenghs);
        }
    }
}
