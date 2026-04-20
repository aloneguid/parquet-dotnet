using System.Numerics;
using System.Runtime.Intrinsics;
using Parquet.Data;

namespace System;

internal static class MemoryExtensions {
    public static int ReadInt32(this Span<byte> span, int offset) {
        if(BitConverter.IsLittleEndian)
            return (int)span[0 + offset] |
                ((int)span[1 + offset] << 8) |
                ((int)span[2 + offset] << 16) |
                ((int)span[3 + offset] << 24);

        return ((int)span[0 + offset] << 24) |
            ((int)span[1 + offset] << 16) |
            ((int)span[2 + offset] << 8) |
            (int)span[3 + offset];
    }

    public static long ReadInt64(this Span<byte> span, int offset) {
        return BitConverter.ToInt64(span.Slice(offset, sizeof(long)));
    }

    public static ReadOnlyMemory<char>? AsNullableReadOnlyMemory(this string? s) {
        if(s == null)
            return null;
        return s.AsMemory();
    }

    public static ReadOnlyMemory<char> AsReadOnlyMemory(this string s) {
        return s.AsMemory();
    }

    public static ReadOnlyMemory<byte>? AsNullableReadOnlyMemory(this byte[]? b) {
        if(b == null)
            return null;
        return b.AsMemory();
    }

    public static ReadOnlyMemory<byte> AsReadOnlyMemory(this byte[] b) {
        return b.AsMemory();
    }

    // All of these could be replaced with generic math, but we don't have access to it due to supporting older than .NET 6

    /// <summary>
    /// Copies the source span to the target one. If the source span is larger than the target span, it will fill the
    /// target and discard the rest. If the source is smaller, only the bytes available will be copied to the target.
    /// </summary>
    public static void CopyWithLimitTo<T>(this Span<T> source, Span<T> target) {
        int copyLength = target.Length;
        if(target.Length > source.Length) {
            copyLength = source.Length;
        }
        source.Slice(0, copyLength).CopyTo(target);
    }
}