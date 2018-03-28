using System;
using System.IO;

namespace Parquet.File.Streams
{
   class WindowedStream : Stream
   {
      private readonly Stream _parent;
      private readonly long _startPos;
      private readonly long _maxLength;
      private int _bytesRead;

      public WindowedStream(Stream parent, long maxLength)
      {
         _parent = parent;
         _startPos = parent.Position;
         _maxLength = maxLength;
      }

      public override bool CanRead => true;

      public override bool CanSeek => false;

      public override bool CanWrite => false;

      public override long Length => Math.Min(_maxLength, _parent.Length - _startPos);

      public override long Position
      {
         get => _bytesRead;
         set => throw new NotSupportedException();
      }

      public override void Flush()
      {
         _parent.Flush();
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         long available = Math.Min(_maxLength - _bytesRead, _parent.Length - _startPos - _bytesRead);

         if (available == 0) return 0;

         if (count > available) count = (int)available;

         int read = _parent.Read(buffer, offset, count);

         _bytesRead += read;

         return read;
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         throw new NotSupportedException();
      }

      public override void SetLength(long value)
      {
         throw new NotSupportedException();
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         throw new NotSupportedException();
      }

      protected override void Dispose(bool disposing)
      {
         //do not dispose base
      }
   }
}
