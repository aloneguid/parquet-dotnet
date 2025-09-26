using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.Extensions {
    static class GuidExtensions {
        public static byte[] ToBigEndianByteArray(this Guid guid) {
#if NET8_0_OR_GREATER
            // .NET 8 has built-in support for big-endian encoding
            // see https://learn.microsoft.com/en-us/dotnet/api/system.guid.tobytearray?view=net-8.0
            return guid.ToByteArray(true);
#else
            byte[] bytes = guid.ToByteArray();
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(bytes, 0, 4);
                Array.Reverse(bytes, 4, 2);
                Array.Reverse(bytes, 6, 2);
            }
            return bytes;
#endif
        }

        public static Guid ToGuidFromBigEndian(this ReadOnlySpan<byte> buffer) {
#if NET8_0_OR_GREATER
            // .NET 8 has built-in support for big-endian encoding
            // see https://learn.microsoft.com/en-us/dotnet/api/system.guid.op_implicit?view=net-8.0
            return new Guid(buffer, true);
#else
            byte[] bytes = buffer.ToArray();
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(bytes, 0, 4);
                Array.Reverse(bytes, 4, 2);
                Array.Reverse(bytes, 6, 2);
            }
            return new Guid(bytes);
#endif
        }
    }
}
