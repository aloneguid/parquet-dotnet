using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using IronCompress;
using Parquet.Extensions;

namespace Parquet.File; 

interface IStreamCompressor {

    /// <summary>
    /// Compresses data from source stream into destination stream and return number of bytes written to destination.
    /// </summary>
    Task<int> CompressAsync(CompressionMethod method, CompressionLevel level, MemoryStream source, Stream destination);

    /// <summary>
    /// Wrap source stream into a decompression stream, which also becomes an owner of the source stream.
    /// </summary>
    Stream Decompress(CompressionMethod method, Stream source, int destinationLength);
}

/// <summary>
/// To be migrated off in future.
/// </summary>
class IronCompressStreamCompressor : IStreamCompressor {

    private static readonly Iron _iron = new();

    public async Task<int> CompressAsync(CompressionMethod method, CompressionLevel level, MemoryStream source, Stream destination) {
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

    public Stream Decompress(CompressionMethod method, Stream source, int destinationLength) {
        // temporary: decompress with IronCompress first, then wrap into MemoryStream
        using IronCompressResult result = _iron.Decompress(ToCodec(method), source.ToByteArray(), destinationLength);
        return new MemoryStream(result.AsSpan().ToArray());
    }
}


static class Compressor {
    public static IStreamCompressor Instance { get; } = new IronCompressStreamCompressor();
}
