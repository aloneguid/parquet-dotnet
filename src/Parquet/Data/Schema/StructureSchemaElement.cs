using System.Collections.Generic;
using Parquet.DataTypes;

namespace Parquet.Data
{
   class StructureSchemaElement : SchemaElement
   {
      public StructureSchemaElement(string name) : base(name, DataType.Structure)
      {
      }

      public IList<SchemaElement> Elements { get; } = new List<SchemaElement>();
   }
}
