using System.IO;

namespace Parquet.File.Data
{
   //note that this may be obsolete in next major version
   interface IDataWriter
   {
      void Write(byte[] buffer, Stream destination);
   }
}
