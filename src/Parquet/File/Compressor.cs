using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using IronCompress;
using Parquet.Extensions;

namespace Parquet.File; 

interface IStreamCompressor {
    Task<int> Compress(CompressionMethod method, CompressionLevel level, MemoryStream source, Stream destination);
}

/// <summary>
/// Existing compressor
/// </summary>
class IronCompressStreamCompressor : IStreamCompressor {

    private static readonly Iron _iron = new();

    public async Task<int> Compress(CompressionMethod method, CompressionLevel level, MemoryStream source, Stream destination) {
        byte[] sourceBytes = source.ToArray();
        using IronCompressResult result = _iron.Compress(ToCodec(method), sourceBytes, compressionLevel: level);
        destination.WriteSpan(result.AsSpan());
        return (int)result.Length;
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

}

static class Compressor {
    private static readonly Iron _iron = new();
    public static IStreamCompressor Instance { get; } = new IronCompressStreamCompressor();

    public static IronCompressResult Decompress(CompressionMethod method, ReadOnlySpan<byte> input, int outLength) => _iron.Decompress(ToCodec(method), input, outLength);

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
}
