namespace Parquet.Test.Util; 

using System;
using System.IO;

/// <summary>
/// Makes stream members virtual instead of abstract, allowing to override only specific behaviors.
/// </summary>
class DelegatedStream : Stream {
    private readonly Stream _master;

    /// <summary>
    /// Creates an instance of non-closeable stream
    /// </summary>
    /// <param name="master"></param>
    public DelegatedStream(Stream master) {
        _master = master ?? throw new ArgumentNullException(nameof(master));
    }

    /// <summary>
    /// Calls <see cref="GetCanRead"/>
    /// </summary>
    public override bool CanRead => GetCanRead();

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    protected virtual bool GetCanRead() {
        return _master.CanRead;
    }

    /// <summary>
    /// Calls <see cref="GetCanSeek"/>
    /// </summary>
    public override bool CanSeek => GetCanSeek();

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    protected virtual bool GetCanSeek() {
        return _master.CanSeek;
    }

    /// <summary>
    /// Calls <see cref="GetCanWrite"/>
    /// </summary>
    public override bool CanWrite => GetCanWrite();

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    protected virtual bool GetCanWrite() {
        return _master.CanWrite;
    }

    /// <summary>
    /// Calls <see cref="GetLength"/>
    /// </summary>
    public override long Length => _master.Length;

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    protected virtual long GetLength() {
        return _master.Length;
    }

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    public override long Position { get => _master.Position; set => _master.Position = value; }

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    public override void Flush() {
        _master.Flush();
    }

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    public override int Read(byte[] buffer, int offset, int count) {
        return _master.Read(buffer, offset, count);
    }

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    public override long Seek(long offset, SeekOrigin origin) {
        return _master.Seek(offset, origin);
    }

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    public override void SetLength(long value) {
        _master.SetLength(value);
    }

    /// <summary>
    /// Delegates to master by default
    /// </summary>
    /// <returns></returns>
    public override void Write(byte[] buffer, int offset, int count) {
        _master.Write(buffer, offset, count);
    }
}