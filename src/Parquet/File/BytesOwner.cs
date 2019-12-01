using System;
using System.IO;

namespace Parquet.File
{
   /// <summary>
   /// Provides a back-reference to an allocator which has created the original byte array.
   /// For instance, if this byte array was rented from a pool, the client won't know how to return it so
   /// he will just dispose it.
   /// </summary>
   class BytesOwner : IDisposable
   {
      private readonly byte[] _bytes;  //original memory buffer
      private readonly int startOffset;
      private readonly Action<byte[]> _disposeAction;

      public BytesOwner(byte[] bytes, int startOffset, Memory<byte> memory, Action<byte[]> disposeAction)
      {
         _bytes = bytes ?? throw new ArgumentNullException(nameof(bytes));
         this.startOffset = startOffset;
         Memory = memory;
         _disposeAction = disposeAction ?? throw new ArgumentNullException(nameof(disposeAction));
      }

      public Memory<byte> Memory { get; }

      /// <summary>
      /// Creates a stream from the current bytes data.
      /// </summary>
      /// <remarks>
      /// Returned stream should be dispose before disposing current object.
      /// </remarks>
      /// <returns>
      /// Stream for bytes represented by current bytes owner instance.
      /// </returns>
      public Stream ToStream()
      {
         return new MemoryStream(_bytes, startOffset, this.Memory.Length);
      }

      public void Dispose()
      {
         _disposeAction(_bytes);
      }
   }
}
