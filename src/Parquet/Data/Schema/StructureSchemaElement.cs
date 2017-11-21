using System.Collections.Generic;

namespace Parquet.Data
{
   public class StructureSchemaElement : Field
   {
      public StructureSchemaElement(string name, bool isArray, params Field[] elements) : base(name, SchemaType.Structure)
      {
         foreach(Field element in elements)
         {
            Elements.Add(element);
         }
      }

      public IList<Field> Elements { get; } = new List<Field>();
   }
}
