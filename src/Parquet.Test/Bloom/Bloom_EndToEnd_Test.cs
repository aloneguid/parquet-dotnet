using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Bloom;

public class Bloom_EndToEnd_Test {
    [Fact]
    public async Task Write_With_Bloom_Then_Read_And_Probe() {
        var field = new DataField<int>("c");
        int[] values = [7, 42, 1000, -5, 7];
        ParquetOptions options = BloomOptions(field.Name);

        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, values.AsMemory());
        }

        stream.Position = 0;
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

        Assert.NotNull(reader.Metadata!.RowGroups[0].Columns[0].MetaData!.BloomFilterOffset);
        Assert.NotNull(reader.Metadata.RowGroups[0].Columns[0].MetaData!.BloomFilterLength);
        Assert.True(rowGroupReader.MightMatchEquals(field, 42));
        Assert.False(rowGroupReader.MightMatchEquals(field, 9999));

        int[] roundTrip = new int[values.Length];
        await rowGroupReader.ReadAsync(field, roundTrip.AsMemory());
        Assert.Equal(values, roundTrip);
    }

    [Fact]
    public async Task Write_Without_Bloom_Does_Not_Prune() {
        var field = new DataField<int>("c");
        int[] values = [1, 2, 3, 4];

        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), stream)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, values.AsMemory());
        }

        stream.Position = 0;
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

        Assert.Null(reader.Metadata!.RowGroups[0].Columns[0].MetaData!.BloomFilterOffset);
        Assert.Null(reader.Metadata.RowGroups[0].Columns[0].MetaData!.BloomFilterLength);
        Assert.True(rowGroupReader.MightMatchEquals(field, 9999));
    }

    [Fact]
    public async Task String_Bloom_Uses_Utf8_Value_Bytes() {
        var field = new DataField<string>("s");
        string?[] values = ["parquet", "bloom", "filter"];
        ParquetOptions options = BloomOptions(field.Name);

        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync(field, (IReadOnlyCollection<string?>)values);
        }

        stream.Position = 0;
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

        Assert.True(rowGroupReader.MightMatchEquals(field, "bloom"));
        Assert.False(rowGroupReader.MightMatchEquals(field, "definitely-not-present"));
    }

    private static ParquetOptions BloomOptions(string columnName) => new() {
        CompressionMethod = CompressionMethod.None,
        BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions> {
            [columnName] = new() { EnableBloomFilters = true }
        }
    };
}
