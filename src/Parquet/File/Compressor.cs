using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance.Buffers;
using IronCompress;
using Parquet.Extensions;
using Snappier;

namespace Parquet.File; 

/// <summary>
/// Data compression interface. Unforunately it's practically impossible to perform streaming in parquet pages.
/// </summary>
interface ICompressor {

    /// <summary>
    /// Compresses data from source stream into destination stream and return number of bytes written to destination.
    /// </summary>
    ValueTask<IMemoryOwner<byte>> CompressAsync(CompressionMethod method, CompressionLevel level, MemoryStream source);

    /// <summary>
    /// Wrap source stream into a decompression stream, which also becomes an owner of the source stream.
    /// </summary>
    ValueTask<IMemoryOwner<byte>> Decompress(CompressionMethod method, Stream source, int destinationLength);
}

/// <summary>
/// To be migrated off in future.
/// </summary>
class IronCompressStreamCompressor : ICompressor {

    private static readonly Iron _iron = new();

    public async ValueTask<IMemoryOwner<byte>> CompressAsync(CompressionMethod method, CompressionLevel level, MemoryStream source) {
        byte[] sourceBytes = source.ToArray();
        using IronCompressResult result = _iron.Compress(ToCodec(method), sourceBytes, compressionLevel: level);
        MemoryOwner<byte> memoryOwner = MemoryOwner<byte>.Allocate((int)result.Length);
        result.AsSpan().CopyTo(memoryOwner.Span);
        return memoryOwner;
    }

    private static Codec ToCodec(CompressionMethod method) {
        switch(method) {
            case CompressionMethod.Snappy:
                return Codec.Snappy;
            case CompressionMethod.Gzip:
                return Codec.Gzip;
            case CompressionMethod.Lzo:
                return Codec.LZO;
            case CompressionMethod.Brotli:
                return Codec.Brotli;
            case CompressionMethod.LZ4:
                return Codec.LZ4;
            case CompressionMethod.Zstd:
                return Codec.Zstd;
            case CompressionMethod.Lz4Raw:
                return Codec.LZ4;
            default:
                throw new NotSupportedException($"{method} not supported");
        }
    }

    public async ValueTask<IMemoryOwner<byte>> Decompress(CompressionMethod method, Stream source, int destinationLength) {
        // temporary: decompress with IronCompress first, then wrap into MemoryStream
        using IronCompressResult result = _iron.Decompress(ToCodec(method), source.ToByteArray(), destinationLength);
        var r = MemoryOwner<byte>.Allocate((int)result.Length);
        result.AsSpan().CopyTo(r.Span);
        return r;
    }
}

class DefaultCompressor : ICompressor {

    private readonly IronCompressStreamCompressor _fallback = new IronCompressStreamCompressor();

    private async ValueTask<IMemoryOwner<byte>> SnappyCompress(MemoryStream source) {
        ReadOnlySpan<byte> src = source.GetBuffer().AsSpan(0, (int)source.Length);
        return Snappy.CompressToMemory(src);
    }

    private async ValueTask<IMemoryOwner<byte>> SnappyDecompress(Stream source) {
        byte[] compressed = source.ToByteArray()!;
        return Snappy.DecompressToMemory(compressed);
    }

    private async ValueTask<IMemoryOwner<byte>> NoneCompress(MemoryStream source) {
        var r = MemoryOwner<byte>.Allocate((int)source.Length);
        source.Position = 0;
        await source.CopyToAsync(r.Memory);
        return r;
    }

    private async ValueTask<IMemoryOwner<byte>> NoneDecompress(Stream source, int destinationLength) {
        var r = MemoryOwner<byte>.Allocate(destinationLength);
        await source.CopyToAsync(r.Memory);
        return r;

    }

    public async ValueTask<IMemoryOwner<byte>> CompressAsync(
        CompressionMethod method, CompressionLevel level, MemoryStream source) {
        switch(method) {
            case CompressionMethod.None:
                return await NoneCompress(source);
            case CompressionMethod.Snappy:
                return await SnappyCompress(source);
            default:
                return await _fallback.CompressAsync(method, level, source);
        }
    }

    public async ValueTask<IMemoryOwner<byte>> Decompress(
        CompressionMethod method, Stream source, int destinationLength) {
        switch(method) {
            case CompressionMethod.None:
                return await NoneDecompress(source, destinationLength);
            case CompressionMethod.Snappy:
                return await SnappyDecompress(source);
            default:
                return await _fallback.Decompress(method, source, destinationLength);
        }

    }
}


static class Compressor {
    public static ICompressor Instance { get; } = new DefaultCompressor();
}
