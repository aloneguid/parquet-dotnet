using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Parquet.Test.Util;

class AsyncOnlyStream : DelegatedStream {
    private readonly Stream _baseStream;
    private readonly int _maxReadSize;

    public AsyncOnlyStream(Stream baseStream, int maxReadSize = int.MaxValue) : base(baseStream) {
        _baseStream = baseStream;
        _maxReadSize = maxReadSize;
    }

    public Action<CancellationToken>? OnReadAsync { get; set; }

    public override int Read(byte[] buffer, int offset, int count) =>
        throw new InvalidOperationException("Synchronous reads are not allowed.");

    public override int Read(Span<byte> buffer) =>
        throw new InvalidOperationException("Synchronous reads are not allowed.");

    public override int ReadByte() =>
        throw new InvalidOperationException("Synchronous reads are not allowed.");

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
        await Task.Yield();
        OnReadAsync?.Invoke(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        return await _baseStream.ReadAsync(buffer.Slice(0, Math.Min(buffer.Length, _maxReadSize)), cancellationToken);
    }
}
