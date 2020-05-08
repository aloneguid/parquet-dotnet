using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using IronSnappy;

namespace Parquet.File.Streams
{
   /// <summary>
   /// In-memory hacky implementation of Snappy streaming as Snappy.Sharp's implementation is a work in progress
   /// </summary>
   class SnappyInMemoryStream : Stream, IMarkStream
   {
      private readonly Stream _parent;
      private readonly CompressionMode _compressionMode;
      private readonly MemoryStream _ms;
      private bool _finishedForWriting;

      public SnappyInMemoryStream(Stream parent, CompressionMode compressionMode)
      {
         _parent = parent;
         _compressionMode = compressionMode;

         if(compressionMode == CompressionMode.Compress)
         {
            _ms = new MemoryStream();
         }
         else
         {
            _ms = DecompressFromStream(parent);
         }
      }

      public override bool CanRead => _compressionMode == CompressionMode.Decompress;

      public override bool CanSeek => false;

      public override bool CanWrite => _compressionMode == CompressionMode.Compress;

      public override long Length => throw new NotSupportedException();

      public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

      public void MarkWriteFinished()
      {
         if (_finishedForWriting) return;

         if(_compressionMode == CompressionMode.Compress)
         {
            byte[] compressed = Snappy.Encode(_ms.ToArray());
            _parent.Write(compressed, 0, compressed.Length);
         }

         _finishedForWriting = true;
      }

      protected override void Dispose(bool disposing)
      {
         Flush();

         base.Dispose(disposing);
      }

      public override int Read(byte[] buffer, int offset, int count)
      {
         return _ms.Read(buffer, offset, count);
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
         _ms.Write(buffer, offset, count);
      }

      private MemoryStream DecompressFromStream(Stream source)
      {
         byte[] buffer = ArrayPool<byte>.Shared.Rent((int)source.Length);
         source.Read(buffer, 0, (int)source.Length);
         byte[] uncompressedBytes = Snappy.Decode(buffer.AsSpan(0, (int)source.Length));
         ArrayPool<byte>.Shared.Return(buffer);
         return new MemoryStream(uncompressedBytes);
      }

      public override void Flush()
      {
         
      }
   }
}
