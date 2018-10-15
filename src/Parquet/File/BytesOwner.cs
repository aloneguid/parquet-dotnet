using System;

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
      private readonly Action<byte[]> _disposeAction;

      public BytesOwner(byte[] bytes, Memory<byte> memory, Action<byte[]> disposeAction)
      {
         _bytes = bytes ?? throw new ArgumentNullException(nameof(bytes));
         Memory = memory;
         _disposeAction = disposeAction ?? throw new ArgumentNullException(nameof(disposeAction));
      }

      public Memory<byte> Memory { get; }

      public void Dispose()
      {
         _disposeAction(_bytes);
      }
   }
}
