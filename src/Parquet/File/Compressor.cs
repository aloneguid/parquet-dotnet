using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance.Buffers;
using IronCompress;
using K4os.Compression.LZ4;
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

    // "None" (no compression) as conversion helper

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

    // "Snappy" compression

    private async ValueTask<IMemoryOwner<byte>> SnappyCompress(MemoryStream source) {
        ReadOnlySpan<byte> src = source.GetBuffer().AsSpan(0, (int)source.Length);
        return Snappy.CompressToMemory(src);
    }

    private async ValueTask<IMemoryOwner<byte>> SnappyDecompress(Stream source) {
        byte[] compressed = source.ToByteArray()!;
        return Snappy.DecompressToMemory(compressed);
    }

    // "Gzip" compression

    private async ValueTask<IMemoryOwner<byte>> GzipCompress(MemoryStream source, CompressionLevel level) {
        // Compress into an in-memory stream and copy into MemoryOwner without extra temporary arrays
        using var ms = new MemoryStream();
        source.Position = 0;
        using (var gzip = new GZipStream(ms, level, leaveOpen: true)) {
            await source.CopyToAsync(gzip);
            await source.FlushAsync();
        }
        int len = (int)ms.Length;
        var owner = MemoryOwner<byte>.Allocate(len);
        // Use underlying buffer to copy bytes to owner
        byte[] buf = ms.GetBuffer();
        new ReadOnlySpan<byte>(buf, 0, len).CopyTo(owner.Span);
        return owner;
    }

    private async ValueTask<IMemoryOwner<byte>> GzipDecompress(Stream source, int destinationLength) {
        var owner = MemoryOwner<byte>.Allocate(destinationLength);
		int copied = 0;
        using var ms = new MemoryStream();
        await source.CopyToAsync(ms);
        ms.Position = 0;
        using(var gzip = new GZipStream(ms, CompressionMode.Decompress, leaveOpen: true)) {
            copied = await gzip.CopyToAsync(owner.Memory);
        }
		if(copied < destinationLength) {
			// IMPORTANT: .Slice() transfers ownership, so owner does not need to be disposed
			owner = owner.Slice(0, copied); 
		}
        return owner;
    }

    private async ValueTask<IMemoryOwner<byte>> GzipDecompress1(Stream source, int destinationLength) {
        var ms = new MemoryStream();
        await source.CopyToAsync(ms);

        ms.Position = 0;
        IMemoryOwner<byte> r0 = await GzipDecompress(ms, destinationLength);

        ms.Position = 0;
        IMemoryOwner<byte> r1 = await _fallback.Decompress(CompressionMethod.Gzip, ms, destinationLength);

        if(!r0.Memory.Span.SequenceEqual(r1.Memory.Span)) {
            throw new InvalidOperationException("Gzip decompression outputs do not match");
        }

        return r0;
    }

    // "LZO" compression

    private async ValueTask<IMemoryOwner<byte>> LzoCompress(MemoryStream source, CompressionLevel level) {
		throw new NotImplementedException("LZO compression is not implemented yet.");
	}

	private async ValueTask<IMemoryOwner<byte>> LzoDecompress(Stream source, int destinationLength) {
		throw new NotImplementedException("LZO decompression is not implemented yet.");
	}

	// "Brotli" compression

#if !NETSTANDARD2_0
	private async ValueTask<IMemoryOwner<byte>> BrotliCompress(MemoryStream source, CompressionLevel level) {
        using var ms = new MemoryStream();
        source.Position = 0;
        using (BrotliStream? brotli = new BrotliStream(ms, level, leaveOpen: true)) {
            await source.CopyToAsync(brotli);
        }
        int len = (int)ms.Length;
        var owner = MemoryOwner<byte>.Allocate(len);
        byte[] buf = ms.GetBuffer();
        new ReadOnlySpan<byte>(buf, 0, len).CopyTo(owner.Span);
        return owner;
    }

    private async ValueTask<IMemoryOwner<byte>> BrotliDecompress(Stream source, int destinationLength) {
        var owner = MemoryOwner<byte>.Allocate(destinationLength);
        using var brotli = new BrotliStream(source, CompressionMode.Decompress, leaveOpen: true);

        byte[] buffer = ArrayPool<byte>.Shared.Rent(81920);
        try {
            int totalRead = 0;
            while(totalRead < destinationLength) {
                int toRead = Math.Min(buffer.Length, destinationLength - totalRead);
                int read = await brotli.ReadAsync(buffer, 0, toRead);
                if(read == 0) break;
                new ReadOnlySpan<byte>(buffer, 0, read).CopyTo(owner.Span.Slice(totalRead, read));
                totalRead += read;
            }
        }
        finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        return owner;
    }
#endif

    // "Zstd" compression

    private async ValueTask<IMemoryOwner<byte>> ZstdCompress(MemoryStream source, CompressionLevel level) {
        int zLevel = level switch {
            CompressionLevel.Optimal => 3,
            CompressionLevel.Fastest => 1,
            CompressionLevel.NoCompression => 1,
#if NET6_0_OR_GREATER
            CompressionLevel.SmallestSize => 19,
#endif
            _ => 0
        };

        using var compressor = new ZstdSharp.Compressor(zLevel);
        ReadOnlySpan<byte> data = source.GetBuffer().AsSpan(0, (int)source.Length);
        Span<byte> compressed = compressor.Wrap(data);
        var owner = MemoryOwner<byte>.Allocate(compressed.Length);
        compressed.CopyTo(owner.Span);
        return owner;
    }

    public async ValueTask<IMemoryOwner<byte>> ZstdDecompress(Stream source, int destinationLength) {
        using var decompressor = new ZstdSharp.Decompressor();
        byte[] compressed = source.ToByteArray()!;
        Span<byte> decompressed = decompressor.Unwrap(compressed, destinationLength);
        var owner = MemoryOwner<byte>.Allocate(decompressed.Length);
        decompressed.CopyTo(owner.Span);
        return owner;
    }

	// "LZ4" compression

	private async ValueTask<IMemoryOwner<byte>> Lz4Compress(MemoryStream source, CompressionLevel level) {
		LZ4Level lz4Level = level switch {
			CompressionLevel.Optimal => LZ4Level.L11_OPT,
			CompressionLevel.Fastest => LZ4Level.L00_FAST,
			CompressionLevel.NoCompression => LZ4Level.L00_FAST,
#if NET6_0_OR_GREATER
			CompressionLevel.SmallestSize => LZ4Level.L12_MAX,
#endif
			_ => LZ4Level.L09_HC
		};

		ReadOnlySpan<byte> data = source.GetBuffer().AsSpan(0, (int)source.Length);
		int maxCompressedSize = LZ4Codec.MaximumOutputSize(data.Length);
		var owner = MemoryOwner<byte>.Allocate(maxCompressedSize);
		int compressedSize = LZ4Codec.Encode(
			data,
			owner.Span,
			level: lz4Level);
		return owner.Slice(0, compressedSize);
	}

	private async ValueTask<IMemoryOwner<byte>> Lz4Decompress(Stream source, int destinationLength) {
		byte[] compressed = source.ToByteArray()!;
		var owner = MemoryOwner<byte>.Allocate(destinationLength);
		LZ4Codec.Decode(compressed, owner.Span);
		return owner;
	}

	public async ValueTask<IMemoryOwner<byte>> CompressAsync(
        CompressionMethod method, CompressionLevel level, MemoryStream source) {
        switch(method) {
            case CompressionMethod.None:
                return await NoneCompress(source);
            case CompressionMethod.Snappy:
                return await SnappyCompress(source);
            case CompressionMethod.Gzip:
                return await GzipCompress(source, level);
			case CompressionMethod.Lzo:
				return await LzoCompress(source, level);
#if !NETSTANDARD2_0
			case CompressionMethod.Brotli:
                return await BrotliCompress(source, level);
#endif
            case CompressionMethod.LZ4:
                return await Lz4Compress(source, level);
			case CompressionMethod.Zstd:
                return await ZstdCompress(source, level);
			case CompressionMethod.Lz4Raw:
				return await Lz4Compress(source, level);
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
            case CompressionMethod.Gzip:
                return await GzipDecompress(source, destinationLength);
			case CompressionMethod.Lzo:
				return await LzoDecompress(source, destinationLength);
#if !NETSTANDARD2_0
			case CompressionMethod.Brotli:
                return await BrotliDecompress(source, destinationLength);
#endif
            case CompressionMethod.LZ4:
                return await Lz4Decompress(source, destinationLength);
			case CompressionMethod.Zstd:
                return await ZstdDecompress(source, destinationLength);
			case CompressionMethod.Lz4Raw:
				return await Lz4Decompress(source, destinationLength);
			default:
                return await _fallback.Decompress(method, source, destinationLength);
        }

    }
}


static class Compressor {
    public static ICompressor Instance { get; } = new DefaultCompressor();
}
