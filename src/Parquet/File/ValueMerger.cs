using Parquet.Data;
using System.Collections;
using System.Collections.Generic;
using System;

namespace Parquet.File
{
   /// <summary>
   /// Responsible for merging values from different parts of column chunk
   /// </summary>
   class ValueMerger
   {
      private readonly int _maxRepetitionLevel;
      private readonly Func<IList> _createEmptyListFunc;
      private IList _values;

      public ValueMerger(int maxRepetitionLevel,
         Func<IList> createEmptyListFunc,
         IList values)
      {
         _maxRepetitionLevel = maxRepetitionLevel;
         _createEmptyListFunc = createEmptyListFunc;
         _values = values;
      }

      /// <summary>
      /// Applies dictionary with indexes and definition levels directly over the column
      /// </summary>
      public IList Apply(IList dictionary, List<int> definitions, List<int> repetitions, List<int> indexes, int maxValues)
      {
         if (dictionary == null && definitions == null && indexes == null && repetitions == null) return _values;  //values are just values

         ApplyDictionary(dictionary, indexes, maxValues);

         ApplyDefinitions(definitions, maxValues);

         ApplyRepetitions(repetitions);

         return _values;
      }

      private void ApplyDictionary(IList dictionary, List<int> indexes, int maxValues)
      {
         //merge with dictionary if present
         if (dictionary == null) return;

         //when dictionary has no indexes
         if (indexes == null) return;

         TrimTail(indexes, maxValues);

         foreach(int index in indexes)
         {
            object value = dictionary[index];
            _values.Add(value);
         }
      }

      private void ApplyDefinitions(List<int> definitions, int maxValues)
      {
         DefinitionPack.InsertDefinitions(_values, definitions);
      }

      private void ApplyRepetitions(List<int> repetitions)
      {
         _values = RepetitionPack.FlatToHierarchy(_maxRepetitionLevel, _createEmptyListFunc, _values, repetitions);
      }

      public static void TrimTail(IList list, int maxValues)
      {
         if (list == null) return;

         if (list.Count > maxValues)
         {
            int diffCount = list.Count - maxValues;
            while (--diffCount >= 0) list.RemoveAt(list.Count - 1); //more effective than copying the list again
         }
      }

      public static void TrimHead(IList list, int maxValues)
      {
         if (list == null) return;

         while (list.Count > maxValues && list.Count > 0)
         {
            list.RemoveAt(0);
         }
      }

      public static void Trim(IList list, int offset, int count)
      {
         TrimHead(list, list.Count - offset);
         TrimTail(list, count);
      }

   }
}
