using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.CompilerServices;
using System.IO;

namespace Parquet.Encodings;

/// <summary>
/// https://github.com/apache/parquet-format/blob/master/Encodings.md#BYTESTREAMSPLIT.
/// Supported Types: FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY. Also, see https://youtu.be/29k7cou8Hdk
/// </summary>
static class ByteStreamSplitEncoder {

    public static bool IsSupported(System.Type t) =>
        t == typeof(float)  ||
        t == typeof(double) ||
        t == typeof(int)    || t == typeof(long)    ||       // native types
        t == typeof(short)  || t == typeof(ushort)  ||       // int32 compatible
        t == typeof(uint)   || t == typeof(ulong);           // int64 compatible

    private static int SafeSizeOf<T>() {
        Type t = typeof(T);
        if(typeof(int) == t || typeof(float) == t) {
            return 4;
        }
        if(typeof(long) == t || typeof(double) == t) {
            return 8;
        }
        throw new NotSupportedException(t.Name);
    }

    public static void Decode<T>(Span<byte> s, Span<T> dest) where T : struct {
        if(ParquetOptions.UseHardwareAcceleration) {
            DecodeHwx(s, dest);
        } else {
            DecodeOnCpu(s, dest);
        }
    }

    public static void DecodeOnCpu<T>(Span<byte> s, Span<T> dest) where T : struct {

        int k = SafeSizeOf<T>();

        if(s.Length < dest.Length * k) {
            throw new ArgumentException("Source span is too small.", nameof(s));
        }

        /*
         * Example: Original data is three 32-bit floats and for simplicity we look at their raw representation.
         *        Element 0      Element 1      Element 2
         * Bytes  AA BB CC DD    00 11 22 33    A3 B4 C5 D6
         * 
         * After applying the transformation, the data has the following representation:
         * Bytes  AA 00 A3 BB 11 B4 CC 22 C5 DD 33 D6
         */

        Span<byte> tmp = stackalloc byte[k];

        for(int i = 0; i < dest.Length; i++) {
            for(int j = 0; j < k; j++) {
                tmp[j] = s[(j * dest.Length) + i];
            }
            dest[i] = MemoryMarshal.Read<T>(tmp);
        }
    }

    public static void DecodeHwx<T>(Span<byte> s, Span<T> dest) where T : struct {
        int k = SafeSizeOf<T>();

        if(s.Length < dest.Length * k) {
            throw new ArgumentException("Source span is too small.", nameof(s));
        }

        // For 4-byte types (int, float), use Vector256 or Vector128 for parallel deinterleaving
        if(k == 4 && Vector256.IsHardwareAccelerated && typeof(T) == typeof(float)) {
            DecodeX_Float256(s, dest);
        } else if(k == 8 && Vector256.IsHardwareAccelerated && typeof(T) == typeof(double)) {
            DecodeX_Double256(s, dest);
        } else {
            // Fallback to CPU version for unsupported configurations
            DecodeOnCpu(s, dest);
        }
    }

    private static void DecodeX_Float256<T>(Span<byte> s, Span<T> dest) where T : struct {
        const int k = 4;
        Span<float> fDest = MemoryMarshal.Cast<T, float>(dest);
        int length = fDest.Length;

        // Process 8 floats at a time (256 bits / 32 bits per float)
        int vectorCount = length / 8 * 8;

        ref byte srcRef = ref MemoryMarshal.GetReference(s);
        ref float dstRef = ref MemoryMarshal.GetReference(fDest);

        Span<byte> tmp = stackalloc byte[32];

        for(int i = 0; i < vectorCount; i += 8) {
            // Load 8 elements worth of bytes (32 bytes total)
            // Layout: [B0_0 B0_1 B0_2 B0_3 B1_0 B1_1 B1_2 B1_3 ... B7_0 B7_1 B7_2 B7_3]
            // Interleaved: B0_0 B1_0 B2_0 ... B7_0 | B0_1 B1_1 B2_1 ... B7_1 | ... | B0_3 B1_3 B2_3 ... B7_3

            for(int j = 0; j < 4; j++) {
                for(int idx = 0; idx < 8; idx++) {
                    tmp[(idx * 4) + j] = Unsafe.Add(ref srcRef, (j * length) + i + idx);
                }
            }

            // Load and convert vectors
            for(int idx = 0; idx < 8; idx++) {
                Unsafe.Add(ref dstRef, i + idx) = MemoryMarshal.Read<float>(tmp.Slice(idx * 4, 4));
            }
        }

        // Process remaining floats sequentially
        Span<byte> tmpScalar = stackalloc byte[k];
        for(int i = vectorCount; i < length; i++) {
            for(int j = 0; j < k; j++) {
                tmpScalar[j] = Unsafe.Add(ref srcRef, (j * length) + i);
            }
            Unsafe.Add(ref dstRef, i) = MemoryMarshal.Read<float>(tmpScalar);
        }
    }

    private static void DecodeX_Double256<T>(Span<byte> s, Span<T> dest) where T : struct {
        const int k = 8;
        Span<double> dDest = MemoryMarshal.Cast<T, double>(dest);
        int length = dDest.Length;

        // Process 4 doubles at a time (256 bits / 64 bits per double)
        int vectorCount = length / 4 * 4;

        ref byte srcRef = ref MemoryMarshal.GetReference(s);
        ref double dstRef = ref MemoryMarshal.GetReference(dDest);

        Span<byte> tmp = stackalloc byte[32];

        for(int i = 0; i < vectorCount; i += 4) {
            // Load 4 elements worth of bytes (32 bytes total)
            for(int j = 0; j < 8; j++) {
                for(int idx = 0; idx < 4; idx++) {
                    tmp[(idx * 8) + j] = Unsafe.Add(ref srcRef, (j * length) + i + idx);
                }
            }

            // Load and convert vectors
            for(int idx = 0; idx < 4; idx++) {
                Unsafe.Add(ref dstRef, i + idx) = MemoryMarshal.Read<double>(tmp.Slice(idx * 8, 8));
            }
        }

        // Process remaining doubles sequentially
        Span<byte> tmpScalar = stackalloc byte[k];
        for(int i = vectorCount; i < length; i++) {
            for(int j = 0; j < k; j++) {
                tmpScalar[j] = Unsafe.Add(ref srcRef, (j * length) + i);
            }
            Unsafe.Add(ref dstRef, i) = MemoryMarshal.Read<double>(tmpScalar);
        }
    }

    public static void Encode<T>(ReadOnlySpan<T> source, Stream dest) where T : struct {
        ArgumentNullException.ThrowIfNull(dest);

        int k = SafeSizeOf<T>();

        /*
         * Reverse of decode: transforms elements back to byte-stream-split format.
         * Example: Original three 32-bit floats
         * Elements  AA BB CC DD    00 11 22 33    A3 B4 C5 D6
         * 
         * After applying the transformation:
         * Bytes  AA 00 A3 BB 11 B4 CC 22 C5 DD 33 D6
         */

        ReadOnlySpan<byte> sourceBytes = MemoryMarshal.AsBytes(source);
        int count = source.Length;

        if(count == 0) {
            return;
        }

        const int maxChunkSize = 8192;
        int chunkSize = Math.Min(count, maxChunkSize);
        byte[] chunk = ArrayPool<byte>.Shared.Rent(chunkSize);

        try {
            for(int j = 0; j < k; j++) {
                int i = 0;
                while(i < count) {
                    int toWrite = Math.Min(chunkSize, count - i);
                    for(int c = 0; c < toWrite; c++) {
                        chunk[c] = sourceBytes[((i + c) * k) + j];
                    }

                    dest.Write(chunk, 0, toWrite);
                    i += toWrite;
                }
            }
        } finally {
            ArrayPool<byte>.Shared.Return(chunk);
        }
    }
}