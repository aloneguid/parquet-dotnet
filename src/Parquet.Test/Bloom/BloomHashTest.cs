using System;
using System.Buffers.Binary;
using Xunit;

namespace Parquet.Test.Bloom {
    public class BloomHashTest {
        [Fact]
        public void Hash_Int32_IsStable() {
            byte[] bytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, 12345);
            Assert.Equal(
                Parquet.Bloom.BloomHasher.HashPlainEncoded(bytes),
                Parquet.Bloom.BloomHasher.HashPlainEncoded(bytes));
        }

        [Fact]
        public void HashPlainEncoded_Slice_Equals_SpanSlice() {
            byte[] data = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            int off = 3, len = 4;
            ulong a = Parquet.Bloom.BloomHasher.HashPlainEncoded(data, off, len);
            ulong b = Parquet.Bloom.BloomHasher.HashPlainEncoded(new ReadOnlySpan<byte>(data, off, len));
            Assert.Equal(a, b);
        }
    }
}
