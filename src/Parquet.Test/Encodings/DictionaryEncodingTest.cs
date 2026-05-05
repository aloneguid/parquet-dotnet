using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encodings;

public class DictionaryEncodingTest : TestBase {

    [Fact]
    public async Task DictionaryEncodingTest2() {
        string?[] data = [
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
            "zzz"
            ];

        var dataField = new DataField<string>("string");
        var parquetSchema = new ParquetSchema(dataField);

        using var stream = new MemoryStream();

        var options = new ParquetOptions();
        options.ColumnEncodingHints["string"] = EncodingHint.Dictionary;
        await using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, stream, options: options)) {
            using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
            await groupWriter.WriteAsync<ReadOnlyMemory<char>>(dataField, data.Select(x => x.AsNullableReadOnlyMemory()).ToArray());
        }

        await using ParquetReader parquetReader = await ParquetReader.CreateAsync(stream);
        List<string?> rdata = await ReadStringColumn(parquetReader, dataField);

        Assert.Equal(data, rdata);
    }

    [Fact]
    public async Task ReadStringDictionaryGeneratedBySpark() {
        using Stream fs = OpenTestFile("string_dictionary_by_spark.parquet");
        await using ParquetReader reader = await ParquetReader.CreateAsync(fs);
        List<string?> values = await ReadStringColumn(reader, reader.Schema.DataFields[0]);

        Assert.Equal(400, values.Count);
        Assert.Equal(Enumerable.Repeat("one", 100).ToArray(), values.Take(100));
        Assert.Equal(Enumerable.Repeat("two", 100).ToArray(), values.Skip(100).Take(100));
        Assert.Equal(Enumerable.Repeat((string?)null, 100).ToArray(), values.Skip(200).Take(100));
        Assert.Equal(Enumerable.Repeat("three", 100).ToArray(), values.Skip(300).Take(100));
    }

    [Fact]
    public async Task AdaptiveSampling_SkipsHighCardinalityColumn() {
        // Generate high-cardinality data (all unique values)
        int count = 5000;
        string?[] data = Enumerable.Range(0, count).Select(i => (string?)Guid.NewGuid().ToString()).ToArray();

        var dataField = new DataField<string>("highCard");
        var schema = new ParquetSchema(dataField);

        using var stream = new MemoryStream();

        // With sampling enabled (1024), the library should detect high cardinality
        // from the sample and skip the full-data scan
        var options = new ParquetOptions {
            DictionaryEncodingSampleSize = 1024
        };
        options.ColumnEncodingHints["highCard"] = EncodingHint.Dictionary;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream, options: options)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        // Verify roundtrip still works correctly
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        string?[] rdata = await ReadStringValuesAsync(reader.OpenRowGroupReader(0), dataField);
        Assert.Equal(data, rdata);
    }

    [Fact]
    public async Task AdaptiveSampling_EnablesLowCardinalityColumn() {
        // Generate low-cardinality data (only 5 unique values repeated)
        int count = 5000;
        string?[] data = Enumerable.Range(0, count).Select(i => (string?)$"category_{i % 5}").ToArray();

        var dataField = new DataField<string>("lowCard");
        var schema = new ParquetSchema(dataField);

        using var stream = new MemoryStream();

        var options = new ParquetOptions {
            DictionaryEncodingSampleSize = 1024
        };
        options.ColumnEncodingHints["lowCard"] = EncodingHint.Dictionary;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream, options: options)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        string?[] rdata = await ReadStringValuesAsync(reader.OpenRowGroupReader(0), dataField);
        Assert.Equal(data, rdata);
    }

    [Fact]
    public async Task PerColumnOverride_ForcesOff() {
        // Low-cardinality data that would normally get dictionary encoded
        string?[] data = Enumerable.Range(0, 100).Select(i => (string?)$"val_{i % 3}").ToArray();

        var dataField = new DataField<string>("status");
        var schema = new ParquetSchema(dataField);

        using var streamWithDict = new MemoryStream();
        using var streamWithoutDict = new MemoryStream();

        // Write with dictionary encoding enabled via ColumnEncodingHints
        var optionsWithDict = new ParquetOptions();
        optionsWithDict.ColumnEncodingHints["status"] = EncodingHint.Dictionary;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamWithDict, options: optionsWithDict)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        // Write with encoding hint explicitly set to Default (no dictionary)
        var optionsOverrideOff = new ParquetOptions();
        optionsOverrideOff.ColumnEncodingHints["status"] = EncodingHint.Default;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamWithoutDict, options: optionsOverrideOff)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        // Both should roundtrip correctly
        await using ParquetReader reader1 = await ParquetReader.CreateAsync(streamWithDict);
        string?[] rdata1 = await ReadStringValuesAsync(reader1.OpenRowGroupReader(0), dataField);
        Assert.Equal(data, rdata1);

        await using ParquetReader reader2 = await ParquetReader.CreateAsync(streamWithoutDict);
        string?[] rdata2 = await ReadStringValuesAsync(reader2.OpenRowGroupReader(0), dataField);
        Assert.Equal(data, rdata2);

        // The override-off file should be larger (no dictionary compression)
        Assert.True(streamWithoutDict.Length > streamWithDict.Length,
            $"Expected override-off ({streamWithoutDict.Length}) > dict-enabled ({streamWithDict.Length})");
    }

    [Fact]
    public async Task PerColumnOverride_ForcesOnWhenNotInSet() {
        // Low-cardinality data
        string?[] data = Enumerable.Range(0, 100).Select(i => (string?)$"val_{i % 3}").ToArray();

        var dataField = new DataField<string>("status");
        var schema = new ParquetSchema(dataField);

        using var streamNoDict = new MemoryStream();
        using var streamOverrideOn = new MemoryStream();

        // Write without dictionary encoding (no encoding hint for column)
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamNoDict)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        // Write with encoding hint forcing dictionary ON
        var optionsOverrideOn = new ParquetOptions();
        optionsOverrideOn.ColumnEncodingHints["status"] = EncodingHint.Dictionary;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamOverrideOn, options: optionsOverrideOn)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        // Both should roundtrip correctly
        await using ParquetReader reader1 = await ParquetReader.CreateAsync(streamNoDict);
        string?[] rdata1 = await ReadStringValuesAsync(reader1.OpenRowGroupReader(0), dataField);
        Assert.Equal(data, rdata1);

        await using ParquetReader reader2 = await ParquetReader.CreateAsync(streamOverrideOn);
        string?[] rdata2 = await ReadStringValuesAsync(reader2.OpenRowGroupReader(0), dataField);
        Assert.Equal(data, rdata2);

        // The override-on file should be smaller (dictionary compression applied)
        Assert.True(streamOverrideOn.Length < streamNoDict.Length,
            $"Expected override-on ({streamOverrideOn.Length}) < no-dict ({streamNoDict.Length})");
    }

    [Fact]
    public async Task MixedCardinality_RoundtripWithAdaptiveSampling() {
        int count = 5000;
        string?[] lowCardData = Enumerable.Range(0, count).Select(i => (string?)$"status_{i % 3}").ToArray();
        string?[] highCardData = Enumerable.Range(0, count).Select(i => (string?)Guid.NewGuid().ToString()).ToArray();

        var lowCardField = new DataField<string>("status");
        var highCardField = new DataField<string>("id");
        var schema = new ParquetSchema(lowCardField, highCardField);

        using var stream = new MemoryStream();

        var options = new ParquetOptions {
            DictionaryEncodingSampleSize = 1024
        };
        options.ColumnEncodingHints["status"] = EncodingHint.Dictionary;
        options.ColumnEncodingHints["id"] = EncodingHint.Dictionary;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, stream, options: options)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(lowCardField, lowCardData);
            await rg.WriteAsync(highCardField, highCardData);
        }

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
        string?[] rLow = await ReadStringValuesAsync(rgr, lowCardField);
        string?[] rHigh = await ReadStringValuesAsync(rgr, highCardField);
        Assert.Equal(lowCardData, rLow);
        Assert.Equal(highCardData, rHigh);
    }

    [Fact]
    public async Task SamplingDisabled_UsesFullScan() {
        // With sampleSize=0, should behave identically to no sampling
        string?[] data = Enumerable.Range(0, 100).Select(i => (string?)$"val_{i % 5}").ToArray();

        var dataField = new DataField<string>("col");
        var schema = new ParquetSchema(dataField);

        using var streamSampled = new MemoryStream();
        using var streamNoSample = new MemoryStream();

        // With sampling
        var optionsSampled = new ParquetOptions {
            DictionaryEncodingSampleSize = 50
        };
        optionsSampled.ColumnEncodingHints["col"] = EncodingHint.Dictionary;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamSampled, options: optionsSampled)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        // Without sampling (sampleSize = 0)
        var optionsNoSample = new ParquetOptions {
            DictionaryEncodingSampleSize = 0
        };
        optionsNoSample.ColumnEncodingHints["col"] = EncodingHint.Dictionary;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, streamNoSample, options: optionsNoSample)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(dataField, data);
        }

        // Both should produce identical output (low cardinality passes both paths)
        Assert.Equal(streamSampled.Length, streamNoSample.Length);

        await using ParquetReader reader = await ParquetReader.CreateAsync(streamNoSample);
        string?[] rdata = await ReadStringValuesAsync(reader.OpenRowGroupReader(0), dataField);
        Assert.Equal(data, rdata);
    }
}