using Parquet.Thrift;
using System.Collections;
using System.IO;

namespace Parquet.File.Values
{
   interface IValuesReader
   {
      void Read(BinaryReader reder, SchemaElement schema, IList destination);
   }
}
