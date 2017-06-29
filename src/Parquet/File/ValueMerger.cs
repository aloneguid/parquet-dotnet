using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.File
{
   /// <summary>
   /// Responsible for merging values from different parts of column chunk
   /// </summary>
   class ValueMerger
   {
      private readonly ParquetColumn _column;

      public ValueMerger(ParquetColumn column)
      {
         _column = column;
      }

      /// <summary>
      /// Applies dictionary with indexes and definition levels directly over the column
      /// </summary>
      public void Apply(IList dictionary, List<int> definitions, List<int> indexes, long maxValues)
      {
         if (dictionary == null && definitions == null && indexes == null) return;  //values are just values

         ApplyDictionary(dictionary, indexes, maxValues);

         ApplyDefinitions(definitions, maxValues);
      }

      private void ApplyDictionary(IList dictionary, List<int> indexes, long maxValues)
      {
         //merge with dictionary if present
         if (dictionary == null) return;

         if (indexes == null) throw new ParquetException("dictionary has no attached index");

         Trim(indexes, maxValues);

         IList values = indexes
            .Select(i => dictionary[i])
            .ToList();

         Trim(values, maxValues);

         _column.Assign(values);
      }

      private void ApplyDefinitions(List<int> definitions, long maxValues)
      {
         if (definitions == null) return;

         Trim(definitions, maxValues);

         int valueIdx = 0;
         IList values = ParquetColumn.CreateValuesList(_column.Schema, out Type systemType);

         foreach (int isDefinedInt in definitions)
         {
            bool isDefined = isDefinedInt != 0;

            if (isDefined)
            {
               values.Add(_column.Values[valueIdx++]);
            }
            else
            {
               values.Add(null);
            }
         }

         Trim(values, maxValues);
         _column.Assign(values);
      }

      private static void Trim(IList list, long maxValues)
      {
         if (list.Count > maxValues)
         {
            int diffCount = list.Count - (int)maxValues;
            while (--diffCount >= 0) list.RemoveAt(list.Count - 1); //more effective than copying the list again
         }
      }

   }
}
