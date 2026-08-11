using System;
using Parquet.Data;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Extensions;

public class StatsComputeTest {

    [Fact]
    public void StringMinMax() {
        ReadOnlySpan<string> span = new string[] { "one", "two", "three" }.AsSpan();

        var dcs = new DataColumnStatistics();
        StatsCompute.Compute(span, dcs);

        Assert.Equal("one", dcs.MinValue);
        Assert.Equal("two", dcs.MaxValue);
    }

    [Fact]
    public void StringMinMax_EmptySpan() {
        ReadOnlySpan<string> span = Array.Empty<string>().AsSpan();

        var dcs = new DataColumnStatistics();
        StatsCompute.Compute(span, dcs);

        Assert.Null(dcs.MinValue);
        Assert.Null(dcs.MaxValue);
    }

    [Fact]
    public void MemoryCharMinMax() {
        ReadOnlySpan<ReadOnlyMemory<char>> span = new ReadOnlyMemory<char>[] {
            "one".AsMemory(), "two".AsMemory(), "three".AsMemory()
        }.AsSpan();

        var dcs = new DataColumnStatistics();
        StatsCompute.Compute(span, dcs);

        Assert.Equal("one", dcs.MinValue);
        Assert.Equal("two", dcs.MaxValue);
    }

    [Fact]
    public void MemoryCharMinMax_EmptySpan() {
        ReadOnlySpan<ReadOnlyMemory<char>> span = Array.Empty<ReadOnlyMemory<char>>().AsSpan();

        var dcs = new DataColumnStatistics();
        StatsCompute.Compute(span, dcs);

        Assert.Null(dcs.MinValue);
        Assert.Null(dcs.MaxValue);
    }

    [Fact]
    public void MemoryCharMinMax_UsesOrdinalComparison() {
        ReadOnlySpan<ReadOnlyMemory<char>> span = new ReadOnlyMemory<char>[] {
            "A".AsMemory(), "a".AsMemory(), "Z".AsMemory()
        }.AsSpan();

        var dcs = new DataColumnStatistics();
        StatsCompute.Compute(span, dcs);

        Assert.Equal("A", dcs.MinValue);
        Assert.Equal("a", dcs.MaxValue);
    }

    [Fact]
    public void StringMinMax_UsesOrdinalComparison() {
        ReadOnlySpan<string> span = new[] { "A", "a", "Z" }.AsSpan();

        var dcs = new DataColumnStatistics();
        StatsCompute.Compute(span, dcs);

        Assert.Equal("A", dcs.MinValue);
        Assert.Equal("a", dcs.MaxValue);
    }
}
