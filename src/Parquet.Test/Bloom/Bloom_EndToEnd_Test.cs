using System;
using System.Collections.Generic;
using System.IO;
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
    /// End-to-end tests for writing and reading columns with Split-Block Bloom filters.
    /// Verifies on-disk metadata (offset/length), in-memory probing, pruning, and data roundtrip.
    /// </summary>
    public sealed class Bloom_EndToEnd_Test {
        private static DataField I32(string name = "c") => new DataField(name, System.Type.GetType("System.Int32")!);
        private static DataField STRING(string name = "s") => new DataField(name, System.Type.GetType("System.String")!);
        private static SchemaElement I32Physical(string name = "c") =>
            new SchemaElement { Name = name, Type = Parquet.Meta.Type.INT32, RepetitionType = FieldRepetitionType.REQUIRED };

        /// <summary>
        /// Writes a single INT32 column with Bloom filters enabled and confirms:
        ///  - BloomFilterOffset/Length are populated in ColumnMetaData,
        ///  - the Bloom can be read back and probed for present/absent values,
        ///  - the column data round-trips correctly through the reader.
        /// </summary>
        [Fact]
        public async Task Write_With_Bloom_Then_Read_And_Probe() {
            // 1) Field + attach to schema
            DataField field = I32();
            var schema = new ParquetSchema(field); // attaches the field

            // 2) Footer must be created with schema + total row count
            int[] values = { 7, 42, 1000, -5, 7 };
            var footer = new ThriftFooter(schema, totalRowCount: values.Length);

            // 3) SchemaElement for the physical type (you can keep your manual one,
            //    or grab it from footer.GetWriteableSchema()[0])
            var se = new SchemaElement {
                Name = field.Name,
                Type = Meta.Type.INT32,
                RepetitionType = FieldRepetitionType.REQUIRED
            };

            // 4) Now safe to construct the DataColumn
            var col = new DataColumn(field, values);

            using var ms = new MemoryStream();

            var writer = new DataColumnWriter(
                ms, footer, se,
                compressionMethod: CompressionMethod.None,
                options: new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                },
                compressionLevel: System.IO.Compression.CompressionLevel.NoCompression,
                keyValueMetadata: null);

            ColumnChunk chunk = await writer.WriteAsync(new FieldPath(field.Name), col);

            // Assert writer populated bloom offsets
            Assert.NotNull(chunk.MetaData);
            Assert.NotNull(chunk.MetaData!.BloomFilterOffset);
            Assert.NotNull(chunk.MetaData!.BloomFilterLength);
            Assert.True(chunk.MetaData!.BloomFilterLength!.Value > 0);

            // Read bloom back from the stream using the metadata offsets
            ms.Position = 0; // BloomFilterIO.ReadFromStream will seek to the offset
            SplitBlockBloomFilter re =
                BloomFilterIO.ReadFromStream(ms, chunk.MetaData!, s => new ThriftCompactProtocolReader(s));

            // Present values should MightContain == true (probabilistic); choose one absent probe
            Assert.True(re.MightContain(PlainLE(42)));
            Assert.True(re.MightContain(PlainLE(7)));
            Assert.False(re.MightContain(PlainLE(999999)));

            // Also test reader-side pruning helper on a real DataColumnReader
            ms.Position = 0;
            var stats = new DataColumnStatistics { NullCount = 0, DistinctCount = 4 };
            var reader = new DataColumnReader(field, ms, chunk, stats, footer, parquetOptions: new ParquetOptions {
                BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
            });

            // Pruning checks
            Assert.True(reader.MightMatchEquals(42));     // present -> might match
            Assert.False(reader.MightMatchEquals(9999));  // clearly absent -> pruned

            // Finally, fully read the column back and verify data
            ms.Position = 0;
            DataColumn roundTripped = await reader.ReadAsync();
            Assert.Equal(values, (int[])roundTripped.Data);
        }

        /// <summary>
        /// Writes the same INT32 column with Bloom filters **disabled** and confirms:
        ///  - no Bloom offset/length recorded,
        ///  - reader exposes no pruning (returns true),
        ///  - data still round-trips.
        /// </summary>
        [Fact]
        public async Task Write_Without_Bloom_No_Metadata_No_Prune() {
            // 1) Field + attach to schema
            DataField field = I32();
            var schema = new Parquet.Schema.ParquetSchema(field);

            // 2) Footer with total row count
            int[] values = { 1, 2, 3, 4 };
            var footer = new ThriftFooter(schema, totalRowCount: values.Length);

            // 3) Physical schema element (or use footer.GetWriteableSchema()[0])
            SchemaElement se = I32Physical(field.Name);

            // 4) Now it's safe to build the DataColumn
            DataColumn col = new DataColumn(field, values);

            using var ms = new MemoryStream();
            var writer = new DataColumnWriter(
                ms, footer, se,
                compressionMethod: CompressionMethod.None,
                options: new ParquetOptions(),
                compressionLevel: System.IO.Compression.CompressionLevel.NoCompression,
                keyValueMetadata: null);

            ColumnChunk chunk = await writer.WriteAsync(new FieldPath(field.Name), col);

            Assert.NotNull(chunk.MetaData);
            Assert.Null(chunk.MetaData!.BloomFilterOffset);
            Assert.Null(chunk.MetaData!.BloomFilterLength);

            ms.Position = 0;
            var stats = new DataColumnStatistics { NullCount = 0 };
            var reader = new DataColumnReader(field, ms, chunk, stats, footer, parquetOptions: new ParquetOptions {
                BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
            });

            // No bloom => pruning helper must not prune
            Assert.True(reader.MightMatchEquals(9999));

            ms.Position = 0;
            DataColumn rt = await reader.ReadAsync();
            Assert.Equal(values, (int[])rt.Data);
        }


        /// <summary>
        /// Writes a BYTE_ARRAY (UTF-8 string) column with blooms enabled and validates
        /// that probes use raw UTF-8 bytes (no length prefix) and pruning works.
        /// </summary>
        [Fact]
        public async Task ByteArray_String_Bloom_EndToEnd() {
            // 1) Field + attach to schema
            DataField field = STRING();
            var schema = new Parquet.Schema.ParquetSchema(field);

            // 2) Footer with total row count
            string[] data = { "parquet", "bloom", "filter" };
            var footer = new ThriftFooter(schema, totalRowCount: data.Length);

            // 3) Physical schema element
            var se = new SchemaElement {
                Name = field.Name,
                Type = Parquet.Meta.Type.BYTE_ARRAY,
                RepetitionType = FieldRepetitionType.REQUIRED
            };

            // 4) Now it's safe to build the DataColumn
            DataColumn col = new DataColumn(field, data);

            using var ms = new MemoryStream();
            var writer = new DataColumnWriter(
                ms, footer, se,
                compressionMethod: CompressionMethod.None,
                options: new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                },
                compressionLevel: System.IO.Compression.CompressionLevel.NoCompression,
                keyValueMetadata: null);

            ColumnChunk chunk = await writer.WriteAsync(new FieldPath(field.Name), col);

            Assert.NotNull(chunk.MetaData!.BloomFilterOffset);
            Assert.True(chunk.MetaData!.BloomFilterLength!.Value > 0);

            // Load bloom and probe exact UTF-8 bytes
            ms.Position = 0;
            SplitBlockBloomFilter bloom =
                BloomFilterIO.ReadFromStream(ms, chunk.MetaData!, s => new ThriftCompactProtocolReader(s));
            Assert.True(bloom.MightContain(System.Text.Encoding.UTF8.GetBytes("parquet")));
            Assert.False(bloom.MightContain(System.Text.Encoding.UTF8.GetBytes("def-not-present")));

            // Reader pruning + full read
            ms.Position = 0;
            var stats = new DataColumnStatistics { NullCount = 0, DistinctCount = 3 };
            var reader = new DataColumnReader(field, ms, chunk, stats, footer, parquetOptions: new ParquetOptions {
                BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { field.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
            });
            Assert.True(reader.MightMatchEquals("bloom"));
            Assert.False(reader.MightMatchEquals("nope"));

            ms.Position = 0;
            DataColumn rt = await reader.ReadAsync();
            Assert.Equal(data, (string[])rt.Data);
        }

        [Fact]
        public async Task WriteRead_WithBloom_MultipleRowGroups() {
            // Single INT32 column, multiple row groups, bloom enabled
            var id = new DataField<int>("id");
            using var ms = new MemoryStream();

            // WRITE
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                new ParquetSchema(id),
                ms,
                new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { id.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                })) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 10, 11, 12 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 20, 21 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 30, 31, 32, 33 }));
                }
            }

            // READ
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(
                ms,
                new ParquetOptions {
                    BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions>() {
                        { id.Name, new ParquetOptions.BloomFilterOptions { EnableBloomFilters = true } }
                    }
                })) {
                Assert.Equal(3, reader.RowGroupCount);

                DataField datafield = reader.Schema.GetDataFields()[0];

                using(ParquetRowGroupReader rg0 = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(3, rg0.RowCount);
                    Assert.True(rg0.MightMatchEquals(id, 10));
                    Assert.False(rg0.MightMatchEquals(id, 9999));
                    DataColumn dc = await rg0.ReadColumnAsync(id);
                    Assert.Equal(new[] { 10, 11, 12 }, (int[])dc.Data);
                }

                using(ParquetRowGroupReader rg1 = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(2, rg1.RowCount);
                    Assert.True(rg1.MightMatchEquals(id, 21));
                    Assert.False(rg1.MightMatchEquals(id, -1));
                    DataColumn dc = await rg1.ReadColumnAsync(id);
                    Assert.Equal(new[] { 20, 21 }, (int[])dc.Data);
                }

                using(ParquetRowGroupReader rg2 = reader.OpenRowGroupReader(2)) {
                    Assert.Equal(4, rg2.RowCount);
                    Assert.True(rg2.MightMatchEquals(id, 32));
                    Assert.False(rg2.MightMatchEquals(id, 0));
                    DataColumn dc = await rg2.ReadColumnAsync(id);
                    Assert.Equal(new[] { 30, 31, 32, 33 }, (int[])dc.Data);
                }
            }
        }


        // ---------- helpers ----------

        private static byte[] PlainLE(int v) {
            byte[] b = BitConverter.GetBytes(v);
            if(!BitConverter.IsLittleEndian)
                Array.Reverse(b);
            return b;
        }
    }
}