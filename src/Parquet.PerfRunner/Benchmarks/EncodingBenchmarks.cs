using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;
using BenchmarkDotNet.Attributes;
using CommunityToolkit.HighPerformance.Buffers;
using Parquet.Encodings;

namespace Parquet.PerfRunner.Benchmarks;

[MediumRunJob]
[MeanColumn]
[MemoryDiagnoser]
[MarkdownExporter]
public class EncodingBenchmarks : BenchmarkBase {

    private IMemoryOwner<bool>? _boolMemory;
    private IMemoryOwner<byte>? _byteStreamSplitEncoded;
    private IMemoryOwner<float>? _byteStreamSplitDecoded;

    [Params(100, 200, 1_000_000)]
    //[Params(100)]
    public int Size;

    [GlobalSetup]
    public void Setup() {
        _boolMemory = CreateBools(Size);
        _byteStreamSplitEncoded = CreateByteStreamSplitEncoded(Size);
        _byteStreamSplitDecoded = MemoryOwner<float>.Allocate(Size);
    }

    [GlobalCleanup]
    public void Cleanup() {
        _boolMemory?.Dispose();
        _byteStreamSplitEncoded?.Dispose();
    }

    //[Benchmark]
    public void PlainBool() {
        if(_boolMemory == null) throw new InvalidOperationException();

        using var ms = new MemoryStream();
        ParquetPlainEncoder.EncodeOnCpu(_boolMemory.Memory.Span, ms);
    }

    //[Benchmark]
    public void PlainBoolX() {
        if(_boolMemory == null) throw new InvalidOperationException();

        using var ms = new MemoryStream();
        ParquetPlainEncoder.EncodeHwx(_boolMemory.Memory.Span, ms);
    }

    //[Benchmark]
    //public void DecodeByteStreamSplit5() {
    //    ByteStreamSplitEncoder.DecodeByteStreamSplit5(_byteStreamSplitEncoded.Memory.Span, _byteStreamSplitDecoded.Memory.Span);
    //}

    [Benchmark]
    public void DecodeByteStreamSplit() {
        ByteStreamSplitEncoder.DecodeOnCpu(_byteStreamSplitEncoded.Memory.Span, _byteStreamSplitDecoded.Memory.Span);
    }

    [Benchmark]
    public void DecodeByteStreamSplitHwx() {
        ByteStreamSplitEncoder.DecodeHwx(_byteStreamSplitEncoded.Memory.Span, _byteStreamSplitDecoded.Memory.Span);
    }


    public static IMemoryOwner<byte> CreateByteStreamSplitEncoded(int count) {
        var floatOwner = MemoryOwner<float>.Allocate(count);
        Span<float> floatSpan = floatOwner.Memory.Span;
        for(int i = 0; i < count; i++) {
            floatSpan[i] = i * 1.5f;
        }

        var byteOwner = MemoryOwner<byte>.Allocate(count * sizeof(float));
        Span<byte> byteSpan = byteOwner.Memory.Span;

        using var ms = new MemoryStream(count * sizeof(float));
        ByteStreamSplitEncoder.Encode(floatSpan, ms);
        ms.GetBuffer().AsSpan(0, (int)ms.Length).CopyTo(byteSpan);

        floatOwner.Dispose();
        return byteOwner;
    }
}