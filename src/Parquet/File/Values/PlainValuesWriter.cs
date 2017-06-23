using Parquet.Thrift;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Parquet.File.Values
{
   class PlainValuesWriter : IValuesWriter
   {
      public void Write(BinaryWriter writer, SchemaElement schema, IList data)
      {
      }
   }
}
