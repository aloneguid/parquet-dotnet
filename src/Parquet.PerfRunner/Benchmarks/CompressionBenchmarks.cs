using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Compression;
using System.Text;
using BenchmarkDotNet.Attributes;
using CommunityToolkit.HighPerformance.Buffers;

namespace Parquet.PerfRunner.Benchmarks;

[MediumRunJob]
[MeanColumn]
[MemoryDiagnoser]
[MarkdownExporter]
public class CompressionBenchmarks : BenchmarkBase {

    private readonly MemoryStream source = new MemoryStream();

    public CompressionBenchmarks() {
    }

    //[Params(CompressionLevel.Optimal, CompressionLevel.Fastest, CompressionLevel.NoCompression, CompressionLevel.SmallestSize)]
    [Params(CompressionLevel.Optimal)]
    public CompressionLevel CompressionLevel { get; set; }

    [GlobalSetup]
    public void SetUp() {
        for(int i = 0; i < 10; i++) {
            byte[] data = new byte[100_000];
            Random.Shared.NextBytes(data);
            source.Write(data, 0, data.Length);
        }
        source.Position = 0;
    }

    [Benchmark(Baseline = true)]
    public async ValueTask<IMemoryOwner<byte>> ZstdSharpPort() {
        int zLevel = CompressionLevel switch {
            CompressionLevel.Optimal => 3,
            CompressionLevel.Fastest => 1,
            CompressionLevel.NoCompression => 1,
            CompressionLevel.SmallestSize => 19,
            _ => 0
        };

        using var compressor = new ZstdSharp.Compressor(zLevel);
        ReadOnlySpan<byte> data = source.GetBuffer().AsSpan(0, (int)source.Length);
        Span<byte> compressed = compressor.Wrap(data);
        var owner = MemoryOwner<byte>.Allocate(compressed.Length);
        compressed.CopyTo(owner.Span);
        return owner;
    }

    //[Benchmark]
    public async ValueTask<IMemoryOwner<byte>> SystemCompressionStream() {
        // Compress into an in-memory stream and copy into MemoryOwner without extra temporary arrays
        using var ms = new MemoryStream();
        source.Position = 0;
        using(var zstd = new ZstandardStream(ms, CompressionLevel, leaveOpen: true)) {
            await source.CopyToAsync(zstd);
            await zstd.FlushAsync();
        }
        int len = (int)ms.Length;
        var owner = MemoryOwner<byte>.Allocate(len);
        if(ms.TryGetBuffer(out ArraySegment<byte> buffer)) {
            buffer.AsSpan(0, len).CopyTo(owner.Span);
        } else {
            ms.ToArray().AsSpan(0, len).CopyTo(owner.Span);
        }
        return owner.Slice(0, len);
    }

    [Benchmark]
    public async ValueTask<IMemoryOwner<byte>> SystemCompressionBuffer() {

        int zLevel = CompressionLevel switch {
            CompressionLevel.Optimal => 3,
            CompressionLevel.Fastest => 1,
            CompressionLevel.NoCompression => 1,
            CompressionLevel.SmallestSize => 19,
            _ => 0
        };

        long maxDestLength = ZstandardEncoder.GetMaxCompressedLength(source.Length);
        MemoryOwner<byte> owner = MemoryOwner<byte>.Allocate((int)maxDestLength);
        bool ok = ZstandardEncoder.TryCompress(source.GetBuffer(), owner.Memory.Span, out int compressedLength, zLevel, 0);
        if (!ok) {
            throw new InvalidOperationException("Compression failed");
        }
        return owner.Slice(0, compressedLength);
    }
}