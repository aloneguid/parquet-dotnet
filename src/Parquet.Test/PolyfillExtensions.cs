#if NETFRAMEWORK
namespace Parquet.Test
{
    using System;
    using System.IO;
    using System.Threading.Tasks;

    internal static class NetFrameworkPolyfills
    {
        extension(BitConverter)
        {
            public static float ToSingle(Span<byte> value) => BitConverter.ToSingle(value.ToArray(), 0);
        }

        extension(System.IO.File)
        {
            public static async Task WriteAllBytesAsync(string path, byte[] bytes)
            {
                using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read, 4096, useAsync: true);
                await fs.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
            }
        }
    }
}

namespace System.Runtime.CompilerServices
{
    internal static class RuntimeHelpers
    {
        public static T[] GetSubArray<T>(T[] array, Range range)
        {
            (int offset, int length) = range.GetOffsetAndLength(array.Length);
            var dest = new T[length];
            Array.Copy(array, offset, dest, 0, length);
            return dest;
        }
    }
}
#endif