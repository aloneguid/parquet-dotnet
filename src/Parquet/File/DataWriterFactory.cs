using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using Snappy.Sharp;

namespace Parquet.File
{
   /// <summary>
   /// part of experiments
   /// </summary>
   static class DataWriterFactory
   {
      public static BinaryWriter CreateWriter(Stream nakedStream, CompressionMethod compressionMethod)
      {
         Stream dest = nakedStream;

         switch(compressionMethod)
         {
            case CompressionMethod.Gzip:
               dest = new GZipStream(dest, CompressionLevel.Optimal, false);
               break;
            case CompressionMethod.Snappy:
               dest = new SnappyStream(dest, CompressionMode.Compress, false, false);
               break;
            default:
               throw new NotImplementedException();
         }

         return new BinaryWriter(dest);
      }
   }
}
