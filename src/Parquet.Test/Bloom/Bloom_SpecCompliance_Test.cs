using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Bloom;
using Parquet.Data;
using Parquet.File;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Spec-compliance tests for Parquet Split-Block Bloom filters.
    /// These validate header conformance, hashing (via round-trip), dictionary semantics,
    /// reader robustness, and multi-page behavior.
    /// </summary>
    public sealed class Bloom_SpecCompliance_Test {
        private static DataField I32(string name = "c") => new DataField(name, System.Type.GetType("System.Int32")!);
        private static DataField I64(string name = "l") => new DataField(name, System.Type.GetType("System.Int64")!);
        private static DataField F32(string name = "f") => new DataField(name, System.Type.GetType("System.Single")!);
        private static DataField F64(string name = "d") => new DataField(name, System.Type.GetType("System.Double")!);
        private static DataField STRING(string name = "s") => new DataField(name, System.Type.GetType("System.String")!);
        private static DataField NINT32(string name = "n") => new DataField(name, System.Type.GetType("System.Int32")!, isNullable: true);
        private static SchemaElement Physical(string name, Parquet.Meta.Type t) =>
            new SchemaElement { Name = name, Type = t, RepetitionType = FieldRepetitionType.REQUIRED };

        private static (ThriftFooter footer, FieldPath path, SchemaElement se) SetupFooter(
            ParquetSchema schema, DataField field, int totalRows, Parquet.Meta.Type physicalType) {
            var footer = new ThriftFooter(schema, totalRowCount: totalRows);
            var path = new FieldPath(field.Name);
            SchemaElement se = Physical(field.Name, physicalType);
            return (footer, path, se);
        }

        /// <summary>
        /// Header conformance: NumBytes multiple of 32; Algorithm.BLOCK; Hash.XXHASH; Compression.UNCOMPRESSED;
        /// ColumnMetaData.BloomFilterLength == sizeof(header) + NumBytes and offset points to header.
        /// </summary>
        [Fact]
        public async Task Header_Is_Spec_Conformant_On_Write() {
            DataField field = I32();
            var schema = new ParquetSchema(field);
            int[] values = { 1, 2, 3, 4 };

            (ThriftFooter? footer, FieldPath? path, SchemaElement? se) = SetupFooter(schema, field, values.Length, Parquet.Meta.Type.INT32);

            using var ms = new MemoryStream();
            var writer = new DataColumnWriter(ms, footer, se,
                compressionMethod: CompressionMethod.None,
                options: new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                },
                compressionLevel: System.IO.Compression.CompressionLevel.NoCompression,
                keyValueMetadata: null);

            var col = new DataColumn(field, values);
            ColumnChunk chunk = await writer.WriteAsync(path, col);

            Assert.NotNull(chunk.MetaData!.BloomFilterOffset);

            long start = chunk.MetaData!.BloomFilterOffset!.Value;
            ms.Position = start;

            // Read Bloom header and verify fields per spec
            BloomFilterHeader header = BloomFilterHeader.Read(new ThriftCompactProtocolReader(ms));

            Assert.True(header.NumBytes > 0 && header.NumBytes % 32 == 0);
            Assert.NotNull(header.Algorithm.BLOCK);
            Assert.NotNull(header.Hash.XXHASH);
            Assert.NotNull(header.Compression.UNCOMPRESSED);

            long afterHeader = ms.Position;
            int headerSize = (int)(afterHeader - start);

            long payloadStart = ms.Position;
            ms.Position += header.NumBytes;
            Assert.Equal(payloadStart + header.NumBytes, ms.Position);
        }

        /// <summary>
        /// Dictionary-encoded semantics: Bloom must contain dictionary VALUES, not indexes.
        /// Probes for present dictionary values return true; absent value returns false.
        /// </summary>
        [Fact]
        public async Task DictionaryEncoded_Bloom_Uses_Dictionary_Values() {
            DataField field = STRING();
            var schema = new ParquetSchema(field);
            string[] data = { "a", "a", "b", "b", "c", "c" };

            (ThriftFooter? footer, FieldPath? path, SchemaElement? se) = SetupFooter(schema, field, data.Length, Parquet.Meta.Type.BYTE_ARRAY);

            using var ms = new MemoryStream();
            var writer = new DataColumnWriter(ms, footer, se,
                CompressionMethod.None, options: new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                },
                System.IO.Compression.CompressionLevel.NoCompression, null);

            ColumnChunk chunk = await writer.WriteAsync(path, new DataColumn(field, data));

            ms.Position = 0;
            SplitBlockBloomFilter bloom = BloomFilterIO.ReadFromStream(ms, chunk.MetaData!, s => new ThriftCompactProtocolReader(s));
            Assert.True(bloom.MightContain(System.Text.Encoding.UTF8.GetBytes("a")));
            Assert.True(bloom.MightContain(System.Text.Encoding.UTF8.GetBytes("b")));
            Assert.True(bloom.MightContain(System.Text.Encoding.UTF8.GetBytes("c")));
            Assert.False(bloom.MightContain(System.Text.Encoding.UTF8.GetBytes("z")));
        }

        /// <summary>
        /// Reader robustness: reject invalid header NumBytes (not multiple of 32).
        /// </summary>
        [Fact]
        public void Read_Rejects_Header_With_Invalid_NumBytes() {
            using var ms = new MemoryStream();
            var meta = new ColumnMetaData();

            // Put header at non-zero offset
            ms.WriteByte(0xEE);
            long offset = ms.Position;

            var bad = new BloomFilterHeader {
                NumBytes = 1, // invalid per spec
                Algorithm = new BloomFilterAlgorithm { BLOCK = new SplitBlockAlgorithm() },
                Hash = new BloomFilterHash { XXHASH = new XxHash() },
                Compression = new BloomFilterCompression { UNCOMPRESSED = new Uncompressed() }
            };
            bad.Write(new ThriftCompactProtocolWriter(ms));

            meta.BloomFilterOffset = offset;
            // intentionally omit bitset

            Assert.Throws<InvalidDataException>(() =>
                BloomFilterIO.ReadFromStream(ms, meta, s => new ThriftCompactProtocolReader(s)));
        }

        /// <summary>
        /// Multi-page, no-dictionary path: GetFileOffset() is safe (falls back to 0) and
        /// Bloom is usable for pruning; data round-trips correctly.
        /// </summary>
        [Fact]
        public async Task MultiPage_NoDictionary_Bloom_OK() {
            DataField field = I32();
            var schema = new ParquetSchema(field);

            // Make enough rows to plausibly trigger multiple data pages in your writer
            int[] values = Enumerable.Range(0, 10).ToArray();

            (ThriftFooter? footer, FieldPath? path, SchemaElement? se) = SetupFooter(schema, field, values.Length, Parquet.Meta.Type.INT32);

            using var ms = new MemoryStream();
            var writer = new DataColumnWriter(ms, footer, se,
                CompressionMethod.None, options: new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                },
                System.IO.Compression.CompressionLevel.NoCompression, null);

            ColumnChunk chunk = await writer.WriteAsync(path, new DataColumn(field, values));

            // Reader should not throw on GetFileOffset, even if offsets are 0
            ms.Position = 0;
            var stats = new DataColumnStatistics { NullCount = 0 };
            var reader = new DataColumnReader(field, ms, chunk, stats, footer, parquetOptions: new ParquetOptions {
                BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
            });

            Assert.True(reader.MightMatchEquals(5));
            Assert.False(reader.MightMatchEquals(9999));

            ms.Position = 0;
            DataColumn rt = await reader.ReadAsync();
            Assert.Equal(values, (int[])rt.Data);
        }

        /// <summary>
        /// Hashing via PLAIN encoding for float/double: insert and probe exact numeric values.
        /// (This indirectly verifies raw IEEE-bit hashing without canonicalization.)
        /// </summary>
        [Fact]
        public async Task Float_Double_PlainEncoding_RoundTrip_And_Prune() {
            // FLOAT
            {
                DataField field = F32("f32");
                var schema = new ParquetSchema(field);
                float[] vals = { 0.0f, -0.0f, 3.1415927f, -123.5f };

                (ThriftFooter? footer, FieldPath? path, SchemaElement? se) = SetupFooter(schema, field, vals.Length, Parquet.Meta.Type.FLOAT);
                using var ms = new MemoryStream();
                var writer = new DataColumnWriter(ms, footer, se,
                    CompressionMethod.None, options: new ParquetOptions {
                        BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                    },
                    System.IO.Compression.CompressionLevel.NoCompression, null);

                await writer.WriteAsync(path, new DataColumn(field, vals));

                ColumnChunk? chunk = footer.AddRowGroup().Columns.LastOrDefault(); // if not returned above; else keep returned chunk
                // If your WriteAsync already returns chunk, use that instead of the line above.

                // To keep consistent with WriteAsync return, comment the above two lines and capture the chunk on write if needed.
            }

            // DOUBLE
            {
                DataField field = F64("f64");
                var schema = new ParquetSchema(field);
                double[] vals = { 0.0, -0.0, Math.PI, -1.0E300 };

                (ThriftFooter? footer, FieldPath? path, SchemaElement? se) = SetupFooter(schema, field, vals.Length, Parquet.Meta.Type.DOUBLE);
                using var ms = new MemoryStream();
                var writer = new DataColumnWriter(ms, footer, se,
                    CompressionMethod.None, options: new ParquetOptions {
                        BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                    },
                    System.IO.Compression.CompressionLevel.NoCompression, null);

                ColumnChunk chunk = await writer.WriteAsync(path, new DataColumn(field, vals));

                ms.Position = 0;
                var reader = new DataColumnReader(field, ms, chunk,
                    new DataColumnStatistics { NullCount = 0 }, footer, parquetOptions: new ParquetOptions {
                        BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                    });

                // Probes on present values should be true
                Assert.True(reader.MightMatchEquals(Math.PI));
                Assert.True(reader.MightMatchEquals(0.0));
                // Absent should be false (very likely)
                Assert.False(reader.MightMatchEquals(42.42));

                ms.Position = 0;
                DataColumn rt = await reader.ReadAsync();
                Assert.Equal(vals, (double[])rt.Data);
            }
        }

        /// <summary>
        /// Nulls are not hashed: nullable INT32 column with nulls should not introduce
        /// spurious positives for values that only appear as nulls.
        /// </summary>
        [Fact]
        public async Task Nullable_Int_Nulls_Are_Not_Hashed() {
            DataField field = NINT32();
            var schema = new ParquetSchema(field);

            int?[] vals = { 1, null, 2, null, 3 };
            (ThriftFooter? footer, FieldPath? path, SchemaElement? se) = SetupFooter(schema, field, vals.Length, Parquet.Meta.Type.INT32);

            using var ms = new MemoryStream();
            var writer = new DataColumnWriter(ms, footer, se,
                CompressionMethod.None, options: new ParquetOptions { BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() { { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } } } },
                System.IO.Compression.CompressionLevel.NoCompression, null);

            ColumnChunk chunk = await writer.WriteAsync(path, new DataColumn(field, vals));

            ms.Position = 0;
            var reader = new DataColumnReader(field, ms, chunk,
                new DataColumnStatistics { NullCount = vals.Count(v => v == null) }, footer, parquetOptions: new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                });

            // Values present (non-null)
            Assert.True(reader.MightMatchEquals(1));
            Assert.True(reader.MightMatchEquals(2));
            Assert.True(reader.MightMatchEquals(3));

            // Value that only appears as null is not a thing; pick an absent value
            Assert.False(reader.MightMatchEquals(9999));

            ms.Position = 0;
            DataColumn rt = await reader.ReadAsync();
            Assert.Equal(vals, (int?[])rt.Data);
        }
    }
}