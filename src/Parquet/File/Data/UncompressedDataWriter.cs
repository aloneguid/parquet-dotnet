using System.IO;

namespace Parquet.File.Data
{
   class UncompressedDataWriter : IDataWriter
   {
      public void Write(byte[] buffer, Stream destination)
      {
         destination.Write(buffer, 0, buffer.Length);
      }
   }
}
