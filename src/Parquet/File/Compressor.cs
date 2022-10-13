using System;
using IronCompress;

namespace Parquet.File {
    static class Compressor {
        private static readonly Iron _iron = new Iron();

        public static DataBuffer Compress(CompressionMethod method, ReadOnlySpan<byte> input) {
            return _iron.Compress(ToCodec(method), input);
        }

        public static DataBuffer Decompress(CompressionMethod method, ReadOnlySpan<byte> input, int outLength) {
            return _iron.Decompress(ToCodec(method), input, outLength);
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
                default:
                    throw new NotSupportedException($"{method} not supported");
            }
        }
    }
}
