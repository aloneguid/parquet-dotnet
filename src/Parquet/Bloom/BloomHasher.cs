using System;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace Parquet.Bloom {
    internal static class BloomHasher {

        /// <summary>
        /// Hash a raw span (already PLAIN-encoded).
        /// </summary>
        public static ulong HashPlainEncoded(ReadOnlySpan<byte> data) => HashToU64(data);

        /// <summary>
        /// Hash a raw slice of a byte[] (already PLAIN-encoded).
        /// </summary>
        public static ulong HashPlainEncoded(byte[] buffer, int offset, int length) {
            if(buffer is null)
                throw new ArgumentNullException(nameof(buffer));
            return HashPlainEncoded(new ReadOnlySpan<byte>(buffer, offset, length));
        }

        private static ulong ToU64BE(ReadOnlySpan<byte> hash8)
            => BinaryPrimitives.ReadUInt64BigEndian(hash8);

        private static ulong HashToU64(ReadOnlySpan<byte> data) {
            byte[] h = XxHash64.Hash(data, seed: 0);
            return ToU64BE(h);
        }
    }
}
