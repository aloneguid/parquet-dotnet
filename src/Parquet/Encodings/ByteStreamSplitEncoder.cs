using System;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.CompilerServices;

namespace Parquet.Encodings;

/// <summary>
/// https://github.com/apache/parquet-format/blob/master/Encodings.md#BYTESTREAMSPLIT
/// Supported Types:
/// - FLOAT
/// - DOUBLE
/// - INT32
/// - INT64
/// - FIXED_LEN_BYTE_ARRAY
/// </summary>
static class ByteStreamSplitEncoder {

    public static void DecodeByteStreamSplit5<T>(Span<byte> s, Span<T> dest) where T : struct {
        int k;
        bool isFloat = typeof(T) == typeof(float);
        if(isFloat) {
            k = 4;
        } else if(typeof(T) == typeof(double)) {
            k = 8;
        } else {
            throw new NotSupportedException(typeof(T).Name);
        }

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
        if(isFloat) {
            Span<float> fDest = MemoryMarshal.Cast<T, float>(dest);
            for(int i = 0; i < dest.Length; i++) {
                for(int j = 0; j < k; j++) {
                    tmp[j] = s[(j * dest.Length) + i];
                }

                fDest[i] = BitConverter.ToSingle(tmp);
            }
        } else {
            Span<double> dDest = MemoryMarshal.Cast<T, double>(dest);
            for(int i = 0; i < dest.Length; i++) {
                for(int j = 0; j < k; j++) {
                    tmp[j] = s[(j * dest.Length) + i];
                }

                dDest[i] = BitConverter.ToDouble(tmp);
            }
        }
    }

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

    public static void DecodeByteStreamSplit<T>(Span<byte> s, Span<T> dest) where T : struct {
        if(ParquetOptions.UseHardwareAcceleration) {
            DecodeByteStreamSplitHwx(s, dest);
        } else {
            DecodeByteStreamSplitOnCpu(s, dest);
        }
    }

    public static void DecodeByteStreamSplitOnCpu<T>(Span<byte> s, Span<T> dest) where T : struct {

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

    public static void DecodeByteStreamSplitHwx<T>(Span<byte> s, Span<T> dest) where T : struct {
        int k = SafeSizeOf<T>();

        if(s.Length < dest.Length * k) {
            throw new ArgumentException("Source span is too small.", nameof(s));
        }

        // For 4-byte types (int, float), use Vector256 or Vector128 for parallel deinterleaving
        if(k == 4 && Vector256.IsHardwareAccelerated && typeof(T) == typeof(float)) {
            DecodeByteStreamSplitX_Float256(s, dest);
        } else if(k == 8 && Vector256.IsHardwareAccelerated && typeof(T) == typeof(double)) {
            DecodeByteStreamSplitX_Double256(s, dest);
        } else {
            // Fallback to CPU version for unsupported configurations
            DecodeByteStreamSplitOnCpu(s, dest);
        }
    }

    private static void DecodeByteStreamSplitX_Float256<T>(Span<byte> s, Span<T> dest) where T : struct {
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

    private static void DecodeByteStreamSplitX_Double256<T>(Span<byte> s, Span<T> dest) where T : struct {
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

    public static void EncodeByteStreamSplit<T>(Span<T> source, Span<byte> dest) where T : struct {

        int k = SafeSizeOf<T>();

        if(dest.Length < source.Length * k) {
            throw new ArgumentException("Destination span is too small.", nameof(dest));
        }

        /*
         * Reverse of decode: transforms elements back to byte-stream-split format.
         * Example: Original three 32-bit floats
         * Elements  AA BB CC DD    00 11 22 33    A3 B4 C5 D6
         * 
         * After applying the transformation:
         * Bytes  AA 00 A3 BB 11 B4 CC 22 C5 DD 33 D6
         */

        Span<byte> tmp = stackalloc byte[k];

        for(int i = 0; i < source.Length; i++) {
            MemoryMarshal.Write(tmp, in source[i]);
            for(int j = 0; j < k; j++) {
                dest[(j * source.Length) + i] = tmp[j];
            }
        }
    }
}