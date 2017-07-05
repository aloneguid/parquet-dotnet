using System;

namespace Parquet.File.Data
{
   class GzipDataReader : IDataReader
   {
      public byte[] Read(byte[] buffer, int offset, int count)
      {
         byte[] source;

         if(offset == 0 && count == buffer.Length)
         {
            source = buffer;
         }
         else
         {
            source = new byte[count];
            Array.Copy(buffer, offset, source, 0, count);
         }

         return source.Ungzip();
      }
   }
}
