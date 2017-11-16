using System.Collections.Generic;
using Parquet.DataTypes;

namespace Parquet.Data
{
   public class StructureSchemaElement : SchemaElement
   {
      public StructureSchemaElement(string name, params SchemaElement[] elements) : base(name, DataType.Structure)
      {
         foreach(SchemaElement element in elements)
         {
            Elements.Add(element);
         }
      }

      public IList<SchemaElement> Elements { get; } = new List<SchemaElement>();
   }
}
