using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Parquet.File
{
   class UtilityStream : Stream
   {
      private readonly Stream _parent;
      private long _startPos;

      public UtilityStream(Stream parent)
      {
         _parent = parent;
      }

      public void ResetStartPos()
      {
         _startPos = _parent.Position;
      }

      public override bool CanRead => throw new NotImplementedException();

      public override bool CanSeek => throw new NotImplementedException();

      public override bool CanWrite => throw new NotImplementedException();

      public override long Length => throw new NotImplementedException();

      public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

      public override void Flush()
      {
         throw new NotImplementedException();
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         throw new NotImplementedException();
      }

      public override long Seek(long offset, SeekOrigin origin)
      {
         throw new NotImplementedException();
      }

      public override void SetLength(long value)
      {
         throw new NotImplementedException();
      }

      public override void Write(byte[] buffer, int offset, int count)
      {
         throw new NotImplementedException();
      }
   }
}
