using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Parquet.Extensions {
    static class StreamExtensions {

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteSpan(this Stream s, Span<byte> span) {
#if NETSTANDARD2_0
            s.Write(span.ToArray(), 0, span.Length);
#else
            s.Write(span);
#endif
        }

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

        public static Task WriteInt32Async(this Stream s, int value) {
            byte[] tmp = BitConverter.GetBytes(value);
            return s.WriteAsync(tmp, 0, sizeof(int));
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
    }
}
