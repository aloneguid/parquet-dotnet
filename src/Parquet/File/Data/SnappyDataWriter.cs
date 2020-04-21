using System.IO;
using IronSnappy;

namespace Parquet.File.Data
{
   class SnappyDataWriter : IDataWriter
   {
      public void Write(byte[] buffer, Stream destination)
      {
         byte[] compressed = Snappy.Encode(buffer);
         destination.Write(compressed, 0, buffer.Length);
      }
   }
}
