using System.IO;

namespace Parquet.File.Data
{
   class UncompressedDataWriter : IDataWriter
   {
      private readonly Stream _stream;

      public UncompressedDataWriter(Stream stream)
      {
         _stream = stream;
      }

      public void Write(byte[] buffer)
      {
         _stream.Write(buffer, 0, buffer.Length);
      }
   }
}
