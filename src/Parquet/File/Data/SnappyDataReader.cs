using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Snappy.Sharp;

namespace Parquet.File.Data
{
   class SnappyDataReader : IDataReader
   {
      private readonly SnappyDecompressor _snappyDecompressor = new SnappyDecompressor();
      public byte[] Read(Stream source, int count)
      {
         byte[] buffer = new byte[count];
         source.Read(buffer, 0, count);
         byte[] uncompressedBytes = _snappyDecompressor.Decompress(buffer, 0, count);
         return uncompressedBytes;
      }
   }
}