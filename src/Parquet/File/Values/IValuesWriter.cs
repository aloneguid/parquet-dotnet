using Parquet.Thrift;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Parquet.File.Values
{
   interface IValuesWriter
   {
      void Write(BinaryWriter writer, SchemaElement schema, IList data);
   }
}
