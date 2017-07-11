using System;
using System.IO;

namespace Parquet.File.Data
{
   class GzipDataWriter : IDataWriter
   {
      public void Write(byte[] buffer, Stream destination)
      {
         byte[] compressed = buffer.Gzip();
         destination.Write(compressed, 0, compressed.Length);
      }
   }
}
