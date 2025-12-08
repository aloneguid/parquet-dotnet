using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance;
using Parquet.Extensions.Streaming;

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
            byte[] tmp = s.ReadBytesExactly(sizeof(int));
            return BitConverter.ToInt32(tmp, 0);
        }

        public static async Task<int> ReadInt32Async(this Stream s) {
            byte[] tmp = await s.ReadBytesExactlyAsync(sizeof(int));
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
            byte[] tmp = s.ReadBytesExactly(sizeof(long));
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

        public static ReadStreamSubStream Sub(this Stream s, long start, long length) =>
            new ReadStreamSubStream(s, start, length);

        public static ReadSpanSubStream Sub(this Memory<byte> memory, long start, long length) =>
            new ReadSpanSubStream(memory.Slice((int)start, (int)length));

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

        /// <summary>
        /// Copies data from stream to memory destination until either the destination is full or the stream ends.
        /// </summary>
        /// <param name="s"></param>
        /// <param name="destination"></param>
        /// <param name="token"></param>
        /// <returns>Number of bytes copied</returns>
        public static async ValueTask<int> CopyToAsync(this Stream s, Memory<byte> destination, CancellationToken token = default) {
#if !NETSTANDARD2_0
            int remaining = destination.Length;
            int copied = 0;
            while(remaining > 0) {
                int bytesRead = await s.ReadAsync(destination.Slice(copied), token);
                if(bytesRead == 0)
                    break;
                copied += bytesRead;
            }
            return copied;
#else
            throw new NotImplementedException();
#endif
        }

        public static async ValueTask CopyToAsync(this Memory<byte> source, Stream destination,
            CancellationToken token = default) {
#if !NETSTANDARD2_0
            await destination.WriteAsync(source);
#else
            throw new NotImplementedException();
#endif
        }
    }
}
