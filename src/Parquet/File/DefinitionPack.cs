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
   static class DefinitionPack
   {

      public static List<int> RemoveNulls(IList values, int maxDefinitionLevel)
      {
         var definitions = new List<int>(values.Count);

         foreach(object value in values)
         {
            if(value == null)
            {
               definitions.Add(0);
            }
            else
            {
               definitions.Add(maxDefinitionLevel);
               throw new NotImplementedException();
               //result.Add(value);
            }
         }

         return definitions;
      }

      public static void InsertDefinitions(IList values, List<int> definitions)
      {
         if (definitions == null || !values.IsNullable()) return;

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
