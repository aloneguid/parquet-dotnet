using System;
using System.IO;

namespace Parquet.Extensions.Streaming;

class ReadStreamSubStream : Stream {
    private readonly Stream _baseStream;
    private readonly long _start;
    private readonly long _length;
    private long _position; // relative to substream start

    public ReadStreamSubStream(Stream baseStream, long start, long length) {
        if(baseStream == null)
            throw new ArgumentNullException(nameof(baseStream));
        if(!baseStream.CanSeek)
            throw new ArgumentException("Base stream must support seeking.");
        if(start < 0 || length < 0)
            throw new ArgumentOutOfRangeException("Start and length must be non-negative.");
        // Removed check for start + length > baseStream.Length to allow for streams that may grow or are not fully written yet.

        _baseStream = baseStream;
        _start = start;
        _length = length;
        _position = 0;
    }

    public override bool CanRead => _baseStream.CanRead;
    public override bool CanSeek => _baseStream.CanSeek;
    public override bool CanWrite => false;
    public override long Length => _length;

    public override long Position {
        get => _position;
        set {
            if(value < 0 || value > _length)
                throw new ArgumentOutOfRangeException(nameof(value), "Position must be within substream range.");
            _position = value;
        }
    }

    public override void Flush() => _baseStream.Flush();

    public override int Read(byte[] buffer, int offset, int count) {
        if(buffer == null)
            throw new ArgumentNullException(nameof(buffer));
        if(offset < 0 || count < 0 || offset + count > buffer.Length)
            throw new ArgumentOutOfRangeException("Invalid offset/count.");

        long remaining = _length - _position;
        if(remaining <= 0)
            return 0;

        if(count > remaining)
            count = (int)remaining;

        _baseStream.Position = _start + _position;
        int read = _baseStream.Read(buffer, offset, count);
        _position += read;
        return read;
    }

    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException("Substream is read-only.");

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
                newPos = _length + offset;
                break;
            default:
                throw new ArgumentException("Invalid SeekOrigin.");
        }

        if(newPos < 0 || newPos > _length)
            throw new IOException("Seek position out of substream range.");

        _position = newPos;
        return _position;
    }

    public override void SetLength(long value) {
        throw new NotSupportedException("Cannot change length of a fixed substream.");
    }
}