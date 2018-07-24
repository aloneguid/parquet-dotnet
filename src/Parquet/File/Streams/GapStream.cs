using System;
using System.IO;

namespace Parquet.File.Streams
{
   class GapStream : Stream, IMarkStream
   {
      private readonly Stream _parent;
      private readonly long? _knownLength;
      private readonly bool _leaveOpen;
      private long _position;

      public GapStream(Stream parent, long? knownLength = null, bool leaveOpen = false)
      {
         _parent = parent;
         _knownLength = knownLength;
         _leaveOpen = leaveOpen;
      }

      public override bool CanRead => _parent.CanRead;

      public override bool CanSeek => _parent.CanSeek;

      public override bool CanWrite => _parent.CanWrite;

      public override long Length => _knownLength ?? _parent.Length;

      public override long Position
      {
         get => _position;
         set
         {
            _parent.Position = value;
            _position = value;
         }
      }

      public override void Flush()
      {
         _parent.Flush();
      }

      public void MarkWriteFinished()
      {
         if(_parent is IMarkStream markStream)
         {
            markStream.MarkWriteFinished();
         }
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         int read = _parent.Read(buffer, offset, count);

         _position += read;

         return read;
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         long pos = _parent.Seek(offset, origin);
         _position = pos;
         return pos;
      }

      public override void SetLength(long value)
      {
         _parent.SetLength(value);
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         _parent.Write(buffer, offset, count);
         _position += count;
      }

      protected override void Dispose(bool disposing)
      {
         if (!_leaveOpen)
         {
            _parent.Dispose();
         }

         base.Dispose(disposing);
      }
   }
}
