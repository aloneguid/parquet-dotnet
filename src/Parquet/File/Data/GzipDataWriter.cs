using System;
using System.IO;
using System.IO.Compression;

namespace Parquet.File.Data
{
   class GzipDataWriter : IDataWriter
   {
      public void Write(byte[] buffer, Stream destination)
      {
         byte[] compressed = Compress(buffer);
         destination.Write(compressed, 0, compressed.Length);
      }

      private static byte[] Compress(byte[] source)
      {
         using (var sourceStream = new MemoryStream(source))
         {
            using (var destinationStream = new MemoryStream())
            {
               Compress(sourceStream, destinationStream);
               return destinationStream.ToArray();
            }
         }
      }

      private static void Compress(Stream source, Stream destination)
      {
         if (source == null) throw new ArgumentNullException(nameof(source));
         if (destination == null) throw new ArgumentNullException(nameof(destination));

         using (var compressor = new GZipStream(destination, CompressionLevel.Optimal, true))
         {
            source.CopyTo(compressor);
            compressor.Flush();
         }
      }

   }
}
