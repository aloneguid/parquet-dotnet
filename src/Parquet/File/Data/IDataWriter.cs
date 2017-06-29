using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.File.Data
{
   interface IDataWriter
   {
      void Write(byte[] buffer);
   }
}
