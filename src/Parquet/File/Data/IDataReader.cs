using System.IO;

namespace Parquet.File.Data
{
   interface IDataReader
   {
      byte[] Read(Stream source, int count);
   }
}