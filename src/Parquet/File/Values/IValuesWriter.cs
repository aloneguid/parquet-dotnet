using Parquet.Data;
using System.Collections;
using System.IO;

namespace Parquet.File.Values
{
   interface IValuesWriter
   {
      /// <summary>
      /// Writes the specified writer.
      /// </summary>
      /// <param name="writer">The writer.</param>
      /// <param name="schema">The schema.</param>
      /// <param name="data">The data.</param>
      /// <param name="extraValues">The extra values which could not be written by this encoding and should be partially
      /// picked up by another encoding.</param>
      /// <returns>
      /// False if this encoding cannot write specified data i.e. data type is not supported
      /// </returns>
      bool Write(BinaryWriter writer, SchemaElement schema, IList data, out IList extraValues);
   }
}
