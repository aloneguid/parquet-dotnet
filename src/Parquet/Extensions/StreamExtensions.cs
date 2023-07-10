using System;
using System.IO;
using System.Threading.Tasks;

namespace Parquet.Extensions {
    static class StreamExtensions {
        public static int ReadInt32(this Stream s) {
            byte[] tmp = new byte[sizeof(int)];
            s.Read(tmp, 0, sizeof(int));
            return BitConverter.ToInt32(tmp, 0);
        }

        public static async Task<int> ReadInt32Async(this Stream s) {
            byte[] tmp = new byte[sizeof(int)];
            await s.ReadAsync(tmp, 0, sizeof(int));
            return BitConverter.ToInt32(tmp, 0);
        }

        public static void WriteInt32(this Stream s, int value) {
            byte[] tmp = BitConverter.GetBytes(value);
            s.Write(tmp, 0, sizeof(int));
        }

        public static long ReadInt64(this Stream s) {
            byte[] tmp = new byte[sizeof(long)];
            s.Read(tmp, 0, sizeof(long));
            return BitConverter.ToInt64(tmp, 0);
        }

        public static byte[] ReadBytesExactly(this Stream s, int count) {
            byte[] tmp = new byte[count];
            int read = 0;
            while(read < count) {
                int r = s.Read(tmp, read, count - read);
                if(r == 0)
                    break;
                else
                    read += r;
            }
            if(read < count)
                throw new IOException($"only {read} out of {count} bytes are available");
            return tmp;
        }

        public static async Task<byte[]> ReadBytesExactlyAsync(this Stream s, int count) {
            byte[] tmp = new byte[count];
#if NET7_0_OR_GREATER
            await s.ReadExactlyAsync(tmp, 0, count);
#else
            int read = 0;
            while(read < count) {
                int r = await s.ReadAsync(tmp, read, count - read);
                if(r == 0)
                    break;
                else
                    read += r;
            }
            if(read < count)
                throw new IOException($"only {read} out of {count} bytes are available");
#endif

            return tmp;
        }

        public static int ReadUnsignedVarInt(this Stream s) {
            int result = 0;
            int shift = 0;

            while(true) {
                int b = s.ReadByte();
                result |= ((b & 0x7F) << shift);
                if((b & 0x80) == 0) break;
                shift += 7;
            }

            return result;
        }

        public static long ReadUnsignedVarLong(this Stream s) {
            long value = 0;
            int i = 0;
            long b;
            while(((b = s.ReadByte()) &0x80) != 0) {
                value |= (b & 0x7F) << i;
                i += 7;
            }
            return value | (b << i);
        }

        public static long ReadZigZagVarLong(this Stream s) {
            long raw = s.ReadUnsignedVarLong();
            long temp = (((raw << 63) >> 63) ^ raw) >> 1;
            return temp ^ (raw & (1L << 63));
        }
    }
}
