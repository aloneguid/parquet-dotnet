using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Thrift;

namespace Parquet.File.Values
{
   class RunLengthBitPackingValuesReader : IValuesReader
   {
      public void Read(BinaryReader reder, SchemaElement schema, IList destination)
      {
         throw new NotImplementedException();
      }
   }
}
