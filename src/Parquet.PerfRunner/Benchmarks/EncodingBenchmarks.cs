using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using BenchmarkDotNet.Attributes;
using Parquet.Encodings;

namespace Parquet.PerfRunner.Benchmarks;

[MediumRunJob]
[MeanColumn]
[MemoryDiagnoser]
[MarkdownExporter]
public class EncodingBenchmarks : BenchmarkBase {

    private IMemoryOwner<bool>? _boolMemory;

    [Params(100, 200, 1_000_000)]
    public int Size;

    [GlobalSetup]
    public void Setup() {
        _boolMemory = CreateBools(Size);
    }

    [GlobalCleanup]
    public void Cleanup() {
        _boolMemory?.Dispose();
    }

    [Benchmark]
    public void PlainBool() {
        if(_boolMemory == null) throw new InvalidOperationException();

        using var ms = new MemoryStream();
        ParquetPlainEncoder.EncodeOnCpu(_boolMemory.Memory.Span, ms);
    }

    [Benchmark]
    public void PlainBoolX() {
        if(_boolMemory == null) throw new InvalidOperationException();

        using var ms = new MemoryStream();
        ParquetPlainEncoder.EncodeHwx(_boolMemory.Memory.Span, ms);
    }

}