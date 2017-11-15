using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.File
{
   /// <summary>
   /// Packs/unpacks definition levels
   /// </summary>
   class DefinitionPack
   {

      public DefinitionPack()
      {
      }

      public IList MergeWithDefinitions(IList values, SchemaElement schema, out List<int> definitions)
      {
         definitions = new List<int>(values.Count);
         IList result = TypeFactory.Create(schema, false);

         foreach(object value in values)
         {
            if(value == null)
            {
               definitions.Add(0);
            }
            else
            {
               definitions.Add(schema.MaxDefinitionLevel);
               result.Add(value);
            }
         }

         return result;
      }

      public void InsertDefinitions(IList values, List<int> definitions)
      {
         if (definitions == null || !TypeFactory.IsNullable(values)) return;

         int valueIdx = 0;

         for(int i = 0; i < definitions.Count; i++)
         {
            bool isDefined = definitions[i] != 0;

            if(!isDefined)
            {
               values.Insert(valueIdx, null);
            }

            valueIdx++;
         }
      }
   }
}
