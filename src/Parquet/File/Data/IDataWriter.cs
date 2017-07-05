using System.IO;

namespace Parquet.File.Data
{
   interface IDataWriter
   {
      void Write(byte[] buffer, Stream destination);
   }
}
