using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.File
{
   /// <summary>
   /// Packs/unpacks repetition levels
   /// </summary>
   class RepetitionPack
   {
      public RepetitionPack()
      {
      }

      public IList HierarchyToFlat(SchemaElement schema, IList hierarchy, out List<int> levels)
      {
         levels = new List<int>();
         IList flatValues = schema.CreateValuesList(0);

         int touched = 0;
         HierarchyToFlat(schema.MaxRepetitionLevel, hierarchy, levels, flatValues, ref touched, 0);

         return flatValues;
      }

      private void HierarchyToFlat(int maxRepetitionLevel, IList list, List<int> levels, IList flatValues, ref int touchedListLevel, int listLevel)
      {
         for (int i = 0; i < list.Count; i++)
         {
            object item = list[i];

            if ((listLevel != maxRepetitionLevel) && (item is IList nestedList))
            {
               HierarchyToFlat(maxRepetitionLevel, nestedList, levels, flatValues, ref touchedListLevel, listLevel + 1);
            }
            else
            {
               flatValues.Add(item);
               levels.Add(touchedListLevel);
            }

            touchedListLevel = listLevel;
         }
      }

      [Obsolete]
      public IList FlatToHierarchy(SchemaElement schema, IList flatValues, List<int> levels)
      {
         return FlatToHierarchy(
            schema.MaxRepetitionLevel,
            () => schema.CreateValuesList(0),
            flatValues,
            levels);
      }

      public IList FlatToHierarchy(int maxRepetitionLevel, Func<IList> createEmptyListFunc, IList flatValues, List<int> levels)
      {
         if (levels == null || maxRepetitionLevel == 0) return flatValues;

         //horizontal list split
         var values = new List<IList>();
         IList[] hl = new IList[maxRepetitionLevel];

         //repetition level indicates where to start to create new lists

         IList chunk = null;
         int lrl = -1;

         for (int i = 0; i < flatValues.Count; i++)
         {
            int rl = levels[i];

            if (lrl != rl)
            {
               CreateNestedLists(maxRepetitionLevel, createEmptyListFunc, hl, rl);
               lrl = rl;
               chunk = hl[hl.Length - 1];

               if (rl == 0)
               {
                  //list at level 0 will be a new element
                  values.Add(hl[0]);
               }
            }

            chunk.Add(flatValues[i]);
         }

         return values;
      }

      private void CreateNestedLists(int maxRepetitionLevel, Func<IList> createEmptyListFunc , IList[] hl, int rl)
      {
         int maxIdx = maxRepetitionLevel - 1;

         //replace lists in chain with new instances
         for (int i = maxIdx; i >= rl; i--)
         {
            IList nl = (i == maxIdx)
               ? createEmptyListFunc()
               : new List<IList>();

            hl[i] = nl;
         }

         //rightest old list now should point to leftest new list
         if (rl > 0 && rl <= maxIdx)
         {
            hl[rl - 1].Add(hl[rl]);
         }

         //chain new lists together
         for (int i = maxIdx - 1; i >= rl; i--)
         {
            hl[i].Add(hl[i + 1]);
         }
      }

   }
}
