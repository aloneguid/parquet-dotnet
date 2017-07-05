using Parquet.Data;
using System.Collections;
using System.IO;

namespace Parquet.File.Values
{
   interface IValuesWriter
   {
      void Write(BinaryWriter writer, SchemaElement schema, IList data);
   }
}
