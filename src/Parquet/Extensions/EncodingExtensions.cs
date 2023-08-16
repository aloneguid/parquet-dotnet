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
            long raw = data.ReadUnsignedVarLong(ref offset);
            return ZigZagDecode(raw);
        }

        #endregion

        #region [ Variable Length Encoding ]

        /// <summary>
        /// Encodes in ULEB128 and returns number of emitted bytes.
        /// https://en.wikipedia.org/wiki/LEB128#Unsigned_LEB128
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ULEB128Encode(this ulong value, Span<byte> dest) {
            int i = 0;
            while(value > 127) {
                byte b = (byte)((value & 0x7F) | 0x80);
                dest[i++] = b;
                value >>= 7;
            }
            dest[i++] = (byte)value;
            return i;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteULEB128(this Stream destination, ulong value) {
            Span<byte> buf = stackalloc byte[10];   // max 10 bytes
            int size = value.ULEB128Encode(buf);
            destination.WriteSpan(buf.Slice(0, size));
        }

        /// <summary>
        /// Read a value using the unsigned, variable int encoding.
        /// </summary>
        public static int ReadUnsignedVarInt(this Span<byte> data, ref int offset) {
            int result = 0;
            int shift = 0;

            while(true) {
                byte b = data[offset++];
                result |= ((b & 0x7F) << shift);
                if((b & 0x80) == 0)
                    break;
                shift += 7;
            }

            return result;
        }

        public static long ReadUnsignedVarLong(this Span<byte> data, ref int offset) {
            long value = 0;
            int i = 0;
            long b;
            while(((b = data[offset++]) & 0x80) != 0) {
                value |= (b & 0x7F) << i;
                i += 7;
            }
            return value | (b << i);
        }

        #endregion

    }
}
