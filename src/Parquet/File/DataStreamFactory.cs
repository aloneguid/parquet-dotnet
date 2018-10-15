using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
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

         byte[] data = BytesPool.Rent(compressedLength);

         switch (compressionMethod)
         {
            case CompressionMethod.None:
               //nothing to do, original data is the raw data
               break;
            case CompressionMethod.Gzip:
               using (var source = new MemoryStream(data, 0, compressedLength))
               {
                  byte[] udata = BytesPool.Rent(uncompressedLength);
                  using (var dest = new MemoryStream(udata, 0, uncompressedLength))
                  {
                     using (var gz = new GZipStream(source, CompressionMode.Decompress))
                     {
                        gz.CopyTo(dest);
                     }
                  }
                  BytesPool.Return(data);
                  data = udata;
               }
               break;
            case CompressionMethod.Snappy:
               throw new NotImplementedException();
            default:
               throw new NotSupportedException("method: " + compressionMethod);

         }

         return new BytesOwner(data, data.AsMemory(0, (int)uncompressedLength), d => BytesPool.Return(d));
      }

      public static Stream CreateReader(Stream nakedStream, Thrift.CompressionCodec compressionCodec, long knownLength)
      {
         if (!_codecToCompressionMethod.TryGetValue(compressionCodec, out CompressionMethod compressionMethod))
            throw new NotSupportedException($"reader for compression '{compressionCodec}' is not supported.");

         return CreateReader(nakedStream, compressionMethod, knownLength);
      }

      private static Stream CreateReader(Stream nakedStream, CompressionMethod compressionMethod, long knownLength)
      {
         Stream dest = nakedStream;

         switch(compressionMethod)
         {
            case CompressionMethod.Gzip:
               dest = new GZipStream(nakedStream, CompressionMode.Decompress, false);
               break;
            case CompressionMethod.Snappy:
               dest = new SnappyInMemoryStream(nakedStream, CompressionMode.Decompress);
               break;
            case CompressionMethod.None:
               dest = nakedStream;
               break;
            default:
               throw new NotImplementedException($"unknown compression method {compressionMethod}");
         }

         return new GapStream(dest, knownLength);
      }
   }
}
