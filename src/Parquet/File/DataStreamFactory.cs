using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using IronSnappy;
using Parquet.File.Streams;

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
         Stream nakedStream,
         CompressionMethod compressionMethod, int compressionLevel,
         bool leaveNakedOpen)
      {
         Stream dest;

/*#if !NET14
         nakedStream = new BufferedStream(nakedStream); //optimise writer performance
#endif*/

         switch (compressionMethod)
         {
            case CompressionMethod.Gzip:
               dest = new GZipStream(nakedStream, ToGzipCompressionLevel(compressionLevel), leaveNakedOpen);
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

      private static CompressionLevel ToGzipCompressionLevel(int compressionLevel)
      {
         switch(compressionLevel)
         {
            case 0:
               return CompressionLevel.NoCompression;
            case 1:
               return CompressionLevel.Fastest;
            case 2:
               return CompressionLevel.Optimal;
            default:
               return CompressionLevel.Optimal;
         }
      }

      public static async Task<BytesOwner> ReadPageDataAsync(Stream nakedStream, Thrift.CompressionCodec compressionCodec,
         int compressedLength, int uncompressedLength)
      {
         if (!_codecToCompressionMethod.TryGetValue(compressionCodec, out CompressionMethod compressionMethod))
            throw new NotSupportedException($"reader for compression '{compressionCodec}' is not supported.");

         int totalBytesRead = 0;
         int currentBytesRead = int.MinValue;
         byte[] data = BytesPool.Rent(compressedLength);
         bool dataRented = true;

         // Some storage solutions (like Azure blobs) might require more than one 'Read' action to read the requested length.
         while (totalBytesRead < compressedLength && currentBytesRead != 0)
         {
            currentBytesRead = await nakedStream.ReadAsync(data, totalBytesRead, compressedLength - totalBytesRead);
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
               byte[] uncompressed = Snappy.Decode(data.AsSpan(0, compressedLength));
               BytesPool.Return(data);
               data = uncompressed;
               dataRented = false;
               break;
            default:
               throw new NotSupportedException("method: " + compressionMethod);
         }

         return new BytesOwner(data, 0, data.AsMemory(0, uncompressedLength), d => BytesPool.Return(d), dataRented);
      }
   }
}
