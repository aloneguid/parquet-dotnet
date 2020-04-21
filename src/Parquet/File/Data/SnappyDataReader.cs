using System.IO;
using IronSnappy;

namespace Parquet.File.Data
{
   class SnappyDataReader : IDataReader
   {
      public byte[] Read(Stream source, int count)
      {
         byte[] buffer = new byte[count];
         source.Read(buffer, 0, count);
         byte[] uncompressedBytes = Snappy.Decode(buffer);
         return uncompressedBytes;
      }
   }
}