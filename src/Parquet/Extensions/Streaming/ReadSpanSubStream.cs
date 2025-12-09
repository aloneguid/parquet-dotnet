using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Parquet.Extensions.Streaming; 

/// <summary>
/// A read-only <see cref="Stream"/> backed by a memory slice. This implementation is zero-copy when
/// constructed with a <see cref="ReadOnlyMemory{Byte}"/>. It intentionally does not copy the underlying data.
///
/// Note about <see cref="ReadOnlySpan{Byte}"/>: a span is a ref struct and cannot be stored on the heap,
/// therefore a stream (a reference type) cannot safely hold a span field. Callers that only have a span
/// should convert it to <see cref="ReadOnlyMemory{Byte}"/> or pass the original array and offset to avoid copying.
/// </summary>
class ReadSpanSubStream : Stream {
    private readonly ReadOnlyMemory<byte> _buffer;
    private long _position; // relative position inside the buffer

    /// <summary>
    /// Creates a new read-only stream over the provided memory without copying.
    /// This is the preferred zero-copy constructor.
    /// </summary>
    /// <param name="source">Source memory to expose as a stream.</param>
    public ReadSpanSubStream(ReadOnlyMemory<byte> source) {
        _buffer = source;
        _position = 0;
    }

    /// <summary>
    /// Obsolete constructor that accepted ReadOnlySpan; kept for discoverability only and throws
    /// to avoid accidental copies. Callers that need zero-copy should pass a <see cref="ReadOnlyMemory{Byte}"/>
    /// or the original array/offset instead.
    /// </summary>
    /// <param name="source">Span to wrap (not supported).</param>
    [Obsolete("ReadOnlySpan<byte> cannot be stored by heap types without copying. Use ReadOnlyMemory<byte> or pass a backing array and offset to avoid copies.")]
    public ReadSpanSubStream(ReadOnlySpan<byte> source) => throw new NotSupportedException("ReadOnlySpan<byte> cannot be captured by a Stream without copying; pass ReadOnlyMemory<byte> or array/offset to avoid copies.");

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;

    public override long Length => _buffer.Length;

    public override long Position {
        get => _position;
        set {
            if(value < 0 || value > Length) throw new ArgumentOutOfRangeException(nameof(value));
            _position = value;
        }
    }

    public override void Flush() {
        // No-op for read-only in-memory stream
    }

    public override int Read(byte[] buffer, int offset, int count) {
        if(buffer == null) throw new ArgumentNullException(nameof(buffer));
        if(offset < 0 || count < 0 || offset + count > buffer.Length) throw new ArgumentOutOfRangeException("Invalid offset/count.");

        long remaining = Length - _position;
        if(remaining <= 0) return 0;

        int toRead = (int)Math.Min(count, remaining);
        _buffer.Span.Slice((int)_position, toRead).CopyTo(buffer.AsSpan(offset, toRead));
        _position += toRead;
        return toRead;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
        if(cancellationToken.IsCancellationRequested) return Task.FromCanceled<int>(cancellationToken);
        int r = Read(buffer, offset, count);
        return Task.FromResult(r);
    }

    public override long Seek(long offset, SeekOrigin origin) {
        long newPos;
        switch(origin) {
            case SeekOrigin.Begin:
                newPos = offset;
                break;
            case SeekOrigin.Current:
                newPos = _position + offset;
                break;
            case SeekOrigin.End:
                newPos = Length + offset;
                break;
            default:
                throw new ArgumentException("Invalid SeekOrigin.", nameof(origin));
        }

        if(newPos < 0 || newPos > Length) throw new IOException("Seek position out of range.");
        _position = newPos;
        return _position;
    }

    public override void SetLength(long value) {
        throw new NotSupportedException("Cannot change length of a fixed span-backed stream.");
    }

    public override void Write(byte[] buffer, int offset, int count) {
        throw new NotSupportedException("Stream is read-only.");
    }

    public override void Close() {
        base.Close();
    }

    public override int ReadByte() {
        if(_position >= Length) return -1;
        byte value = _buffer.Span[(int)_position];
        _position++;
        return value;
    }
}
