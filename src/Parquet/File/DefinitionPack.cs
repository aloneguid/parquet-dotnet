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

      /// <summary>
      /// 
      /// </summary>
      /// <param name="values">Values to compress. This operation modifies the list</param>
      /// <param name="maxDefinitionLevel"></param>
      /// <returns>Definitions for the input values</returns>
      public static List<int> RemoveNulls(IList values, int maxDefinitionLevel)
      {
         var definitions = new List<int>(values.Count);

         for(int i = values.Count - 1; i >= 0; i--)
         {
            object value = values[i];
            if(value == null)
            {
               definitions.Add(0);
               values.RemoveAt(i);
            }
            else
            {
               definitions.Add(maxDefinitionLevel);
            }
         }

         definitions.Reverse();
         return definitions;
      }

      public static List<bool> InsertDefinitions(IList values, int maxDefinitionLevel, List<int> definitions)
      {
         if (definitions == null || !values.IsNullable()) return null;

         var noValueFlags = new List<bool>();

         for(int i = 0; i < definitions.Count; i++)
         {
            int def = definitions[i];
            bool hasValue = true;

            if(def == 0)
            {
               values.Insert(i, null);
            }
            else if(def != maxDefinitionLevel)
            {
               values.Insert(i, null); //stil need to insert something to keep consistent length
               hasValue = false;
            }

            noValueFlags.Add(hasValue);
         }

         return noValueFlags;
      }
   }
}
