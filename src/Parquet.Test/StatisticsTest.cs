using System;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test;

public class StatisticsTest : TestBase {

    private async Task AssertDistinctStatForBasicDataTypeAsync(
        Type type,
        Array data,
        long nullCount,
        object? min,
        object? max) {

        var schema = new ParquetSchema(new DataField("id", type));
        DataField id = schema.GetDataFields()[0];

        DataColumn? rc = await WriteReadSingleColumn(id, data);

        Assert.Equal(data.Length, rc!.CalculateRowCount());
        Assert.Equal(nullCount, rc.Statistics.NullCount);
        Assert.Equal(min, rc.Statistics.MinValue);
        Assert.Equal(max, rc.Statistics.MaxValue);
    }

    [Fact]
    public Task Distinct_stat_for_int_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(int),
        new int[] { 4, 2, 1, 3, 5, 1, 4 },
        0,
        1,
        5);

    [Fact]
    public Task Distinct_stat_for_nullable_int_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(int?),
        new int?[] { 4, 2, 1, 3, 1, null, 4 },
        1,
        1,
        4);

    [Fact]
    public Task Distinct_stat_for_string_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(string),
        new string[] { "one", "two", "one" },
        0,
        "one",
        "two");

    [Fact]
    public Task Distinct_stat_for_float_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(float),
        new float[] { 1.23f, 2.1f, 0.5f, 0.5f },
        0,
        0.5f,
        2.1f);

    [Fact]
    public Task Distinct_stat_for_double_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(double),
        new double[] { 1.23D, 2.1D, 0.5D, 0.5D },
        0,
        0.5D,
        2.1D);

    [Fact]
    public Task Distinct_stat_for_datetime_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(DateTime),
        new DateTime[] {
            new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Utc)
        },
        0,
        new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Utc),
        new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Utc));

    [Fact]
    public Task Distinct_stat_for_datetime_unknown_kind_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(DateTime),
        new DateTime[] {
            new DateTime(2019, 12, 16),
            new DateTime(2019, 12, 16),
            new DateTime(2019, 12, 15),
            new DateTime(2019, 12, 17)
        },
        0,
        new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Utc),
        new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Utc));

    [Fact]
    public Task Distinct_stat_for_datetime_local_kind_data() => AssertDistinctStatForBasicDataTypeAsync(
        typeof(DateTime),
        new DateTime[] {
            new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Local),
            new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Local),
            new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Local),
            new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Local)
        },
        0,
        new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Local).ToUniversalTime(),
        new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Local).ToUniversalTime());
}