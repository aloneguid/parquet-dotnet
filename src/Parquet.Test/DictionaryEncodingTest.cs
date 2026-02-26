using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class DictionaryEncodingTest : TestBase {

        [Fact]
        public async Task DictionaryEncodingTest2() {
            string?[] data = new string?[]
            {
            "xxx",
            "xxx",
            "xxx",
            "xxx",
            "xxx",
            null,
            "yyy",
            "yyy",
            "yyy",
            string.Empty,
            "yyy",
            "yyy",
            null,
            null,
            "zzz",
            "zzz",
            "zzz",
            "zzz",
            };

            var dataField = new DataField<string>("string");
            var parquetSchema = new ParquetSchema(dataField);

            using var stream = new MemoryStream();

            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, stream, formatOptions: new ParquetOptions() { UseDictionaryEncoding = true })) {
                using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
                await groupWriter.WriteColumnAsync(new DataColumn(dataField, data));
            }

            using ParquetReader parquetReader = await ParquetReader.CreateAsync(stream);
            using ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0);
            Data.DataColumn dataColumn = await groupReader.ReadColumnAsync(dataField);

            Assert.Equal(data, dataColumn.Data);
        }

        [Fact]
        public async Task ReadStringDictionaryGeneratedBySpark() {
            using Stream fs = OpenTestFile("string_dictionary_by_spark.parquet");
            using ParquetReader reader = await ParquetReader.CreateAsync(fs);

            DataColumn[] cols = await reader.ReadEntireRowGroupAsync(0);
            Assert.Single(cols);
            DataColumn c0 = cols[0];

            Assert.Equal(400, c0.NumValues);
            Assert.Equal(Enumerable.Repeat("one", 100).ToArray(), c0.AsSpan<string>(0, 100).ToArray());
            Assert.Equal(Enumerable.Repeat("two", 100).ToArray(), c0.AsSpan<string>(100, 100).ToArray());
            Assert.Equal(Enumerable.Repeat((string?)null, 100).ToArray(), c0.AsSpan<string>(200, 100).ToArray());
            Assert.Equal(Enumerable.Repeat("three", 100).ToArray(), c0.AsSpan<string>(300, 100).ToArray());
        }

        [Fact]
        public async Task AdaptiveSampling_SkipsHighCardinalityColumn() {
            // Generate high-cardinality data (all unique values)
            int count = 5000;
            string[] data = Enumerable.Range(0, count).Select(i => Guid.NewGuid().ToString()).ToArray();

            var dataField = new DataField<string>("highCard");
            var schema = new ParquetSchema(dataField);

            using var stream = new MemoryStream();

            // With sampling enabled (default 1024), the library should detect high cardinality
            // from the sample and skip the full-data HashSet build
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream,
                formatOptions: new ParquetOptions {
                    UseDictionaryEncoding = true,
                    DictionaryEncodingSampleSize = 1024
                })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            // Verify roundtrip still works correctly
            using ParquetReader reader = await ParquetReader.CreateAsync(stream);
            DataColumn[] cols = await reader.ReadEntireRowGroupAsync(0);
            Assert.Equal(data, cols[0].Data);
        }

        [Fact]
        public async Task AdaptiveSampling_EnablesLowCardinalityColumn() {
            // Generate low-cardinality data (only 5 unique values repeated)
            int count = 5000;
            string[] data = Enumerable.Range(0, count).Select(i => $"category_{i % 5}").ToArray();

            var dataField = new DataField<string>("lowCard");
            var schema = new ParquetSchema(dataField);

            using var stream = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream,
                formatOptions: new ParquetOptions {
                    UseDictionaryEncoding = true,
                    DictionaryEncodingSampleSize = 1024
                })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            using ParquetReader reader = await ParquetReader.CreateAsync(stream);
            DataColumn[] cols = await reader.ReadEntireRowGroupAsync(0);
            Assert.Equal(data, cols[0].Data);
        }

        [Fact]
        public async Task PerColumnOverride_ForcesOff() {
            // Low-cardinality data that would normally get dictionary encoded
            string[] data = Enumerable.Range(0, 100).Select(i => $"val_{i % 3}").ToArray();

            var dataField = new DataField<string>("status");
            var schema = new ParquetSchema(dataField);

            using var streamWithDict = new MemoryStream();
            using var streamWithoutDict = new MemoryStream();

            // Write without override (dictionary enabled by default)
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamWithDict,
                formatOptions: new ParquetOptions { UseDictionaryEncoding = true })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            // Write with per-column override forcing dictionary OFF
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamWithoutDict,
                formatOptions: new ParquetOptions {
                    UseDictionaryEncoding = true,
                    ColumnDictionaryEncodings = new Dictionary<string, bool> { ["status"] = false }
                })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            // Both should roundtrip correctly
            using ParquetReader reader1 = await ParquetReader.CreateAsync(streamWithDict);
            DataColumn[] cols1 = await reader1.ReadEntireRowGroupAsync(0);
            Assert.Equal(data, cols1[0].Data);

            using ParquetReader reader2 = await ParquetReader.CreateAsync(streamWithoutDict);
            DataColumn[] cols2 = await reader2.ReadEntireRowGroupAsync(0);
            Assert.Equal(data, cols2[0].Data);

            // The override-off file should be larger (no dictionary compression)
            Assert.True(streamWithoutDict.Length > streamWithDict.Length,
                $"Expected override-off ({streamWithoutDict.Length}) > dict-enabled ({streamWithDict.Length})");
        }

        [Fact]
        public async Task PerColumnOverride_ForcesOnWhenGlobalOff() {
            // Low-cardinality data
            string[] data = Enumerable.Range(0, 100).Select(i => $"val_{i % 3}").ToArray();

            var dataField = new DataField<string>("status");
            var schema = new ParquetSchema(dataField);

            using var streamGlobalOff = new MemoryStream();
            using var streamOverrideOn = new MemoryStream();

            // Write with global OFF and no override
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamGlobalOff,
                formatOptions: new ParquetOptions { UseDictionaryEncoding = false })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            // Write with global OFF but per-column override ON
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamOverrideOn,
                formatOptions: new ParquetOptions {
                    UseDictionaryEncoding = false,
                    ColumnDictionaryEncodings = new Dictionary<string, bool> { ["status"] = true }
                })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            // Both should roundtrip correctly
            using ParquetReader reader1 = await ParquetReader.CreateAsync(streamGlobalOff);
            DataColumn[] cols1 = await reader1.ReadEntireRowGroupAsync(0);
            Assert.Equal(data, cols1[0].Data);

            using ParquetReader reader2 = await ParquetReader.CreateAsync(streamOverrideOn);
            DataColumn[] cols2 = await reader2.ReadEntireRowGroupAsync(0);
            Assert.Equal(data, cols2[0].Data);

            // The override-on file should be smaller (dictionary compression applied)
            Assert.True(streamOverrideOn.Length < streamGlobalOff.Length,
                $"Expected override-on ({streamOverrideOn.Length}) < global-off ({streamGlobalOff.Length})");
        }

        [Fact]
        public async Task MixedCardinality_RoundtripWithAdaptiveSampling() {
            int count = 5000;
            string[] lowCardData = Enumerable.Range(0, count).Select(i => $"status_{i % 3}").ToArray();
            string[] highCardData = Enumerable.Range(0, count).Select(i => Guid.NewGuid().ToString()).ToArray();
            int[] intData = Enumerable.Range(0, count).Select(i => i % 10).ToArray();

            var lowCardField = new DataField<string>("status");
            var highCardField = new DataField<string>("id");
            var intField = new DataField<int>("category");
            var schema = new ParquetSchema(lowCardField, highCardField, intField);

            using var stream = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream,
                formatOptions: new ParquetOptions {
                    UseDictionaryEncoding = true,
                    DictionaryEncodingSampleSize = 1024
                })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(lowCardField, lowCardData));
                await rg.WriteColumnAsync(new DataColumn(highCardField, highCardData));
                await rg.WriteColumnAsync(new DataColumn(intField, intData));
            }

            using ParquetReader reader = await ParquetReader.CreateAsync(stream);
            DataColumn[] cols = await reader.ReadEntireRowGroupAsync(0);
            Assert.Equal(lowCardData, cols[0].Data);
            Assert.Equal(highCardData, cols[1].Data);
            Assert.Equal(intData, cols[2].Data);
        }

        [Fact]
        public async Task SamplingDisabled_UsesFullScan() {
            // With sampleSize=0, should behave identically to no sampling
            string[] data = Enumerable.Range(0, 100).Select(i => $"val_{i % 5}").ToArray();

            var dataField = new DataField<string>("col");
            var schema = new ParquetSchema(dataField);

            using var streamSampled = new MemoryStream();
            using var streamNoSample = new MemoryStream();

            // With sampling
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamSampled,
                formatOptions: new ParquetOptions {
                    UseDictionaryEncoding = true,
                    DictionaryEncodingSampleSize = 50
                })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            // Without sampling (sampleSize = 0)
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamNoSample,
                formatOptions: new ParquetOptions {
                    UseDictionaryEncoding = true,
                    DictionaryEncodingSampleSize = 0
                })) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(dataField, data));
            }

            // Both should produce identical output (low cardinality passes both paths)
            Assert.Equal(streamSampled.Length, streamNoSample.Length);

            using ParquetReader reader = await ParquetReader.CreateAsync(streamNoSample);
            DataColumn[] cols = await reader.ReadEntireRowGroupAsync(0);
            Assert.Equal(data, cols[0].Data);
        }
    }
}