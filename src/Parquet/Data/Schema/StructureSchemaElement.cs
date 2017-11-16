using System.Collections.Generic;

namespace Parquet.Data
{
   public class StructureSchemaElement : SchemaElement
   {
      public StructureSchemaElement(string name, bool isArray, params SchemaElement[] elements) : base(name, DataType.Structure, true, isArray)
      {
         foreach(SchemaElement element in elements)
         {
            Elements.Add(element);
         }
      }

      public IList<SchemaElement> Elements { get; } = new List<SchemaElement>();
   }
}
