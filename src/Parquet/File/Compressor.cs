using System;
using System.IO.Compression;
using IronCompress;

namespace Parquet.File {
    static class Compressor {
        private static readonly Iron _iron = new();

        public static IronCompressResult Compress(CompressionMethod method, ReadOnlySpan<byte> input, CompressionLevel compressionLevel) => _iron.Compress(ToCodec(method), input, compressionLevel: compressionLevel);

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
}
