using System;
using System.IO;

namespace Parquet.File.Data
{
   class GzipDataReader : IDataReader
   {
      public byte[] Read(Stream source, int count)
      {
         var srcBytes = new byte[count];
         source.Read(srcBytes, 0, srcBytes.Length);
         return srcBytes.Ungzip();
      }
   }
}
