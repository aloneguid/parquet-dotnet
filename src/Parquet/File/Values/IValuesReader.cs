using Parquet.Data;
using System.Collections;
using System.IO;

namespace Parquet.File.Values
{
   interface IValuesReader
   {
      void Read(BinaryReader reader, SchemaElement schema, IList destination, long maxValues);
   }
}
