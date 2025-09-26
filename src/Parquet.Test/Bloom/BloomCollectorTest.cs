using System;
using System.Linq;
using Parquet.Bloom;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Verifies that <see cref="BloomCollector"/> inserts PLAIN-encoded bytes
    /// (without variable-length prefixes) into a <see cref="SplitBlockBloomFilter"/>
    /// for various Parquet physical types.
    /// </summary>
    public sealed class BloomCollectorTest {
        /// <summary>
        /// Ensures that <see cref="BloomCollector.AddString"/> uses UTF-8 bytes of the string
        /// (without any length prefix), per the Parquet bloom filter spec.
        /// </summary>
        [Fact]
        public void AddString_Uses_Utf8_Bytes_NoLength() {
            int blocks = 64;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            string s = "héłło world 👋";
            byte[] utf8 = Encoding.UTF8.GetBytes(s);

            viaCollector.AddString(s);
            manual.Insert(utf8);

            Assert.True(viaCollector.Filter.MightContain(utf8));
            Assert.True(manual.MightContain(utf8));
        }

        /// <summary>
        /// Ensures that <see cref="BloomCollector.AddByteArray"/> inserts the byte content as-is
        /// (no length prefix), matching manual insertion.
        /// </summary>
        [Fact]
        public void AddByteArray_AsIs_NoLength() {
            int blocks = 32;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            byte[] payload = Enumerable.Range(0, 16).Select(i => (byte)((i * 7) + 3)).ToArray();

            viaCollector.AddByteArray(payload);
            manual.Insert(payload);

            Assert.True(viaCollector.Filter.MightContain(payload));
            Assert.True(manual.MightContain(payload));
        }

        /// <summary>
        /// Verifies that <see cref="BloomCollector.AddFixed"/> inserts bytes as-is, appropriate for
        /// FIXED_LEN_BYTE_ARRAY columns.
        /// </summary>
        [Fact]
        public void AddFixed_AsIs() {
            int blocks = 32;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            byte[] fixedBytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0xAA, 0x55 };

            viaCollector.AddFixed(fixedBytes);
            manual.Insert(fixedBytes);

            Assert.True(viaCollector.Filter.MightContain(fixedBytes));
            Assert.True(manual.MightContain(fixedBytes));
        }

        /// <summary>
        /// Confirms that null inputs are ignored by the collector and do not modify the filter.
        /// Compares the bitset before and after a series of null insertions.
        /// </summary>
        [Fact]
        public void Nulls_Are_Ignored() {
            int blocks = 16;
            var viaCollector = new BloomCollector(blocks);

            byte[] before = viaCollector.Filter.ToByteArray();

            viaCollector.AddBoolean(null);
            viaCollector.AddInt32(null);
            viaCollector.AddInt64(null);
            viaCollector.AddFloat(null);
            viaCollector.AddDouble(null);
            viaCollector.AddString(null);
            viaCollector.AddByteArray(null);
            viaCollector.AddFixed(null);

            byte[] after = viaCollector.Filter.ToByteArray();

            Assert.Equal(before, after);
        }
    }
}