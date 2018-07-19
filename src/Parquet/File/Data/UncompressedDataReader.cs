using System.IO;

namespace Parquet.File.Data
{
   class UncompressedDataReader : IDataReader
   {
      public byte[] Read(Stream source, int count)
      {
         byte[] result = new byte[count];

         source.Read(result, 0, count);

         return result;
      }
   }
}
