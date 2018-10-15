using System;

namespace Parquet.File
{
   class BytesOwner : IDisposable
   {
      private readonly byte[] _bytes;
      private readonly Action<byte[]> _disposeAction;

      public BytesOwner(byte[] bytes, Memory<byte> memory, Action<byte[]> disposeAction)
      {
         _bytes = bytes;
         Memory = memory;
         _disposeAction = disposeAction;
      }

      public Memory<byte> Memory { get; }

      public void Dispose()
      {
         _disposeAction(_bytes);
      }
   }
}
