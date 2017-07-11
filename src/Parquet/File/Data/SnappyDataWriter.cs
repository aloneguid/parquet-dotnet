using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Snappy.Sharp;

namespace Parquet.File.Data
{
    class SnappyDataWriter : IDataWriter
    {
       private readonly SnappyCompressor _snappyCompressor = new SnappyCompressor();
       public void Write(byte[] buffer, Stream destination)
       {
          var uncompressedLength = buffer.Length;
          var compressed = new byte[_snappyCompressor.MaxCompressedLength(uncompressedLength)];
          _snappyCompressor.Compress(buffer, 0, uncompressedLength, compressed);
          destination.Write(compressed, 0, compressed.Length);
       }
    }
}
