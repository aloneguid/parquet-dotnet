using System;
using System.IO;
using Parquet.Bloom;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Reader-side round-trip tests for Parquet Bloom Filters.
    /// Loads a filter using <see cref="ColumnMetaData.BloomFilterOffset"/> and
    /// probes it to verify correctness and error handling.
    /// </summary>
    public sealed class BloomFilterIO_ReaderTest {
        private static ThriftCompactProtocolWriter MakeWriter(Stream s) => new ThriftCompactProtocolWriter(s);
        private static ThriftCompactProtocolReader MakeReader(Stream s) => new ThriftCompactProtocolReader(s);

        /// <summary>
        /// Writes a <see cref="SplitBlockBloomFilter"/> (header + bitset) to a stream,
        /// records the offset in <see cref="ColumnMetaData.BloomFilterOffset"/>,
        /// then reads it back and verifies probes for present/absent values.
        /// Ensures the reader seeks to the recorded offset and reconstructs the filter correctly.
        /// </summary>
        [Fact]
        public void Read_RoundTrip_Probes_Work() {
            SplitBlockBloomFilter f = new SplitBlockBloomFilter(16);
            f.Insert(Encoding.ASCII.GetBytes("parquet"));
            f.Insert(Encoding.ASCII.GetBytes("bloom"));
            f.Insert(Encoding.ASCII.GetBytes("filter"));

            ColumnMetaData meta = new ColumnMetaData();

            using(MemoryStream ms = new MemoryStream()) {
                ms.WriteByte(0x42); // padding to move offset

                (long off, int len) = BloomFilterIO.WriteToStream(ms, f, meta, MakeWriter);

                // Seek somewhere else to prove Read seeks to offset
                ms.Seek(0, SeekOrigin.Begin);

                SplitBlockBloomFilter re = BloomFilterIO.ReadFromStream(ms, meta, MakeReader);

                Assert.True(re.MightContain(Encoding.ASCII.GetBytes("parquet")));
                Assert.True(re.MightContain(Encoding.ASCII.GetBytes("bloom")));
                Assert.True(re.MightContain(Encoding.ASCII.GetBytes("filter")));
                Assert.False(re.MightContain(Encoding.ASCII.GetBytes("definitely-not-present")));
            }
        }

        /// <summary>
        /// Verifies that attempting to read a bloom filter without
        /// <see cref="ColumnMetaData.BloomFilterOffset"/> set results in an
        /// <see cref="InvalidOperationException"/>. Guards against misuse of the API.
        /// </summary>
        [Fact]
        public void Read_Throws_When_No_Offset() {
            using(MemoryStream ms = new MemoryStream()) {
                ColumnMetaData meta = new ColumnMetaData();
                Assert.Throws<InvalidOperationException>(() => BloomFilterIO.ReadFromStream(ms, meta, MakeReader));
            }
        }

        /// <summary>
        /// Crafts an invalid bloom header with <c>NumBytes = 1</c> (not a multiple of 32),
        /// writes just the header at a non-zero offset, and asserts that reading fails with
        /// <see cref="InvalidDataException"/>. Validates header sanity checks before bitset read.
        /// </summary>
        [Fact]
        public void Read_Rejects_Invalid_Header_NumBytes() {
            // Hand-craft a header with NumBytes = 1 (invalid), then try to read.
            using(MemoryStream ms = new MemoryStream()) {
                ColumnMetaData meta = new ColumnMetaData();

                // write at non-zero position
                ms.WriteByte(0x00);
                long offset = ms.Position;

                BloomFilterHeader bad = new BloomFilterHeader();
                bad.NumBytes = 1; // invalid (not multiple of 32)
                bad.Algorithm = new BloomFilterAlgorithm { BLOCK = new SplitBlockAlgorithm() };
                bad.Hash = new BloomFilterHash { XXHASH = new XxHash() };
                bad.Compression = new BloomFilterCompression { UNCOMPRESSED = new Uncompressed() };

                ThriftCompactProtocolWriter writer = new ThriftCompactProtocolWriter(ms);
                bad.Write(writer);

                meta.BloomFilterOffset = offset;
                // No bitset written

                Assert.Throws<InvalidDataException>(() => BloomFilterIO.ReadFromStream(ms, meta, s => new ThriftCompactProtocolReader(s)));
            }
        }

        /// <summary>
        /// Ensures writer populates BloomFilterOffset/Length and serialized bytes
        /// contain the expected NumBytes plus some header overhead.
        /// </summary>
        [Fact]
        public void Writer_Populates_Meta_And_Writes_Bytes() {
            var f = new SplitBlockBloomFilter(8);
            f.Insert(Encoding.ASCII.GetBytes("alpha"));

            var meta = new ColumnMetaData();
            using var ms = new MemoryStream();
            ms.WriteByte(0xCC);

            (long Offset, int Length) _ = BloomFilterIO.WriteToStream(ms, f, meta, s => new ThriftCompactProtocolWriter(s));

            Assert.True(meta.BloomFilterOffset.HasValue);
            Assert.True(meta.BloomFilterLength.HasValue);
            Assert.True(meta.BloomFilterLength.Value >= f.NumberOfBlocks * 32);
        }
    }
}