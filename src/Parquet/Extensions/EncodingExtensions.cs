using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace Parquet.Extensions {
    static class EncodingExtensions {

        #region [ Bit Width ]

        public static int GetBitWidth(this int value) {
            for(int i = 0; i < 64; i++) {
                if(value == 0)
                    return i;
                value >>= 1;
            }

            return 1;
        }

        public static int GetBitWidth(this long value) {
            for(int i = 0; i < 64; i++) {
                if(value == 0)
                    return i;
                value >>= 1;
            }

            return 1;
        }

        #endregion

        #region [ ZigZag ]

        public static long ZigZagEncode(this long num) {
            return (num >> 63) ^ (num << 1);
        }

        public static long ZigZagDecode(this long raw) {
            long temp = (((raw << 63) >> 63) ^ raw) >> 1;
            return temp ^ (raw & (1L << 63));
        }

        public static long ReadZigZagVarLong(this Span<byte> data, ref int offset) {
            long raw = (long)data.ULEB128Decode(ref offset);
            return ZigZagDecode(raw);
        }

        #endregion

        #region [ Variable Length Encoding ]

        /// <summary>
        /// Encodes in ULEB128 and returns number of emitted bytes.
        /// https://en.wikipedia.org/wiki/LEB128#Unsigned_LEB128
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ULEB128Encode(this ulong value, Span<byte> dest, ref int offset) {
            while(value > 127) {
                byte b = (byte)((value & 0x7F) | 0x80);
                dest[offset++] = b;
                value >>= 7;
            }
            dest[offset++] = (byte)value;
        }

        /// <summary>
        /// Decodes to unsigned long from span of bytes using ULEB128 encoding.
        /// </summary>
        /// <param name="data">Span to read from</param>
        /// <param name="offset">Reference to start offset in the span. It is updated as the value is read.</param>
        /// <returns></returns>
        public static ulong ULEB128Decode(this Span<byte> data, ref int offset) {
            long result = 0;
            long b;
            int shift = 0;

            while(true) {
                b = data[offset++];
                result |= ((b & 0x7F) << shift);
                if((b & 0x80) == 0)
                    break;
                shift += 7;
            }

            return (ulong)result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteULEB128(this Stream destination, ulong value) {
            Span<byte> buf = stackalloc byte[10];   // max 10 bytes
            int offset = 0;
            value.ULEB128Encode(buf, ref offset);
            destination.WriteSpan(buf.Slice(0, offset));
        }

        #endregion

        #region Leading Zeros
        public static int NumberOfLeadingZerosInt(this int num) {
            if(num <= 0)
                return num == 0 ? 32 : 0;
            int n = 31;
            if(num >= 1 << 16) { n -= 16; num >>>= 16; }
            if(num >= 1 << 8) { n -= 8; num >>>= 8; }
            if(num >= 1 << 4) { n -= 4; num >>>= 4; }
            if(num >= 1 << 2) { n -= 2; num >>>= 2; }
            return n - (num >>> 1);
        }

        public static int NumberOfLeadingZerosLong(this long num) {
            int x = (int)(num >>> 32);
            return x == 0 ? 32 + ((int)num).NumberOfLeadingZerosInt()
                    : x.NumberOfLeadingZerosInt();
        }
        #endregion

    }
}
