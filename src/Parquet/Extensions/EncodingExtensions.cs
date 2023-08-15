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

        public static uint ZigZagEncode(this int num) {
            return (uint)((num >> 31) ^ (num << 1));
        }


        public static ulong ZigZagEncode(this long num) {
            return (ulong)((num >> 63) ^ (num << 1));
        }


        #endregion

        #region [ Variable Length Encoding ]

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteUnsignedVarInt(this int value, Span<byte> dest, ref int consumed) {
            while(value > 127) {
                byte b = (byte)((value & 0x7F) | 0x80);

                dest[consumed++] = b;

                value >>= 7;
            }

            dest[consumed++] = (byte)value;
        }

        public static int WriteUnsignedVarInt(this Stream destination, int value) {
            int consumed = 0;
            Span<byte> buf = stackalloc byte[4];
            value.WriteUnsignedVarInt(buf, ref consumed);
            destination.WriteSpan(buf.Slice(0, consumed));
            return consumed;
        }

        public static int WriteUnsignedVarLong(this Stream destination, ulong value) {
            int consumed = 0;
            Span<byte> buf = stackalloc byte[8];
            while(value > 127) {
                byte b = (byte)((value & 0x7F) | 0x80);

                buf[consumed++] = b;

                value >>= 7;
            }

            buf[consumed++] = (byte)value;
#if NETSTANDARD2_0
            destination.Write(buf.Slice(0, consumed).ToArray(), 0, consumed);
#else
            destination.Write(buf.Slice(0, consumed));
#endif
            return consumed;
        }


        #endregion

    }
}
