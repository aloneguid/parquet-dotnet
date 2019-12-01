using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using Parquet.File.Streams;
using Snappy.Sharp;

namespace Parquet.File
{
   static class DataStreamFactory
   {
      private static ArrayPool<byte> BytesPool = ArrayPool<byte>.Shared;

      private static readonly Dictionary<CompressionMethod, Thrift.CompressionCodec> _compressionMethodToCodec = 
         new Dictionary<CompressionMethod, Thrift.CompressionCodec>
      {
         { CompressionMethod.None, Thrift.CompressionCodec.UNCOMPRESSED },
         { CompressionMethod.Gzip, Thrift.CompressionCodec.GZIP },
         { CompressionMethod.Snappy, Thrift.CompressionCodec.SNAPPY }
      };

      private static readonly Dictionary<Thrift.CompressionCodec, CompressionMethod> _codecToCompressionMethod =
         new Dictionary<Thrift.CompressionCodec, CompressionMethod>
         {
            { Thrift.CompressionCodec.UNCOMPRESSED, CompressionMethod.None },
            { Thrift.CompressionCodec.GZIP, CompressionMethod.Gzip },
            { Thrift.CompressionCodec.SNAPPY, CompressionMethod.Snappy }
         };

      // this will eventually disappear once we fully migrate to System.Memory
      public static GapStream CreateWriter(
         Stream nakedStream, CompressionMethod compressionMethod,
         bool leaveNakedOpen)
      {
         Stream dest;

         switch(compressionMethod)
         {
            case CompressionMethod.Gzip:
               dest = new GZipStream(nakedStream, CompressionLevel.Optimal, leaveNakedOpen);
               leaveNakedOpen = false;
               break;
            case CompressionMethod.Snappy:
               dest = new SnappyInMemoryStream(nakedStream, CompressionMode.Compress);
               leaveNakedOpen = false;
               break;
            case CompressionMethod.None:
               dest = nakedStream;
               break;
            default:
               throw new NotImplementedException($"unknown compression method {compressionMethod}");
         }
         
         return new GapStream(dest, leaveOpen: leaveNakedOpen);
      }

      public static BytesOwner ReadPageData(Stream nakedStream, Thrift.CompressionCodec compressionCodec,
         int compressedLength, int uncompressedLength)
      {
         if (!_codecToCompressionMethod.TryGetValue(compressionCodec, out CompressionMethod compressionMethod))
            throw new NotSupportedException($"reader for compression '{compressionCodec}' is not supported.");

         int totalBytesRead = 0;
         int currentBytesRead = int.MinValue;
         byte[] data = BytesPool.Rent(compressedLength);

         // Some storage solutions (like Azure blobs) might require more than one 'Read' action to read the requested length.
         while (totalBytesRead < compressedLength && currentBytesRead != 0)
         {
            currentBytesRead = nakedStream.Read(data, totalBytesRead, compressedLength - totalBytesRead);
            totalBytesRead += currentBytesRead;
         }

         if (totalBytesRead != compressedLength)
         {
            throw new ParquetException($"expected {compressedLength} bytes in source stream but could read only {totalBytesRead}");
         }

         switch (compressionMethod)
         {
            case CompressionMethod.None:
               //nothing to do, original data is the raw data
               break;
            case CompressionMethod.Gzip:
               using (var source = new MemoryStream(data, 0, compressedLength))
               {
                  byte[] unGzData = BytesPool.Rent(uncompressedLength);
                  using (var dest = new MemoryStream(unGzData, 0, uncompressedLength))
                  {
                     using (var gz = new GZipStream(source, CompressionMode.Decompress))
                     {
                        gz.CopyTo(dest);
                     }
                  }
                  BytesPool.Return(data);
                  data = unGzData;
               }
               break;
            case CompressionMethod.Snappy:
               var snappy = new SnappyDecompressor();
               byte[] unSnapData = snappy.Decompress(BytesPool, data, 0, compressedLength);
               BytesPool.Return(data);
               data = unSnapData;
               break;
            default:
               throw new NotSupportedException("method: " + compressionMethod);
         }

         return new BytesOwner(data, 0, data.AsMemory(0, (int)uncompressedLength), d => BytesPool.Return(d));
      }
   }
}
