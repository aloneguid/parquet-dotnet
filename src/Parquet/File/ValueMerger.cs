using Parquet.Data;
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
      private readonly SchemaElement _schema;
      private readonly ParquetOptions _formatOptions;
      private IList _values;

      public ValueMerger(SchemaElement schema, ParquetOptions formatOptions, IList values)
      {
         _schema = schema;
         _formatOptions = formatOptions;
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

         IList values = indexes
            .Select(i => dictionary[i])
            .ToList();

         TrimTail(values, maxValues);

         foreach (object el in values) _values.Add(el);
      }

      private void ApplyDefinitions(List<int> definitions, int maxValues)
      {
         if (definitions == null) return;

         TrimTail(definitions, maxValues);

         int valueIdx = 0;
         IList values = TypeFactory.Create(_schema, _formatOptions, true);

         foreach (int isDefinedInt in definitions)
         {
            bool isDefined = isDefinedInt != 0;

            if (isDefined)
            {
               values.Add(_values[valueIdx++]);
            }
            else
            {
               values.Add(null);
            }
         }

         _values = values;
      }

      private void ApplyRepetitions(List<int> repetitions)
      {
         if (repetitions == null || _schema.MaxRepetitionLevel == 0) return;

         //horizontal list split
         var values = new List<IList>();
         IList[] hl = new IList[_schema.MaxRepetitionLevel];

         //repetition level indicates where to start to create new lists

         IList chunk = null;
         int lrl = -1;

         for(int i = 0; i < _values.Count; i++)
         {
            int rl = repetitions[i];

            if (lrl != rl)
            {
               CreateLists(hl, rl);
               lrl = rl;
               chunk = hl[hl.Length - 1];

               if(rl == 0)
               {
                  //list at level 0 will be a new element
                  values.Add(hl[0]);
               }
            }

            chunk.Add(_values[i]);
         }

         _values = values;
      }

      private void CreateLists(IList[] hl, int rl)
      {
         int maxIdx = _schema.MaxRepetitionLevel - 1;

         //replace lists in chain with new instances
         for(int i = maxIdx; i >= rl; i--)
         {
            IList nl = (i == maxIdx)
               ? TypeFactory.Create(_schema, _formatOptions)
               : new List<IList>();

            hl[i] = nl;
         }

         //rightest old list now should point to leftest new list
         if(rl > 0 && rl <= maxIdx)
         {
            hl[rl - 1].Add(hl[rl]);
         }

         //chain new lists together
         for(int i = maxIdx - 1; i>= rl; i--)
         {
            hl[i].Add(hl[i + 1]);
         }
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

         while (list.Count > maxValues)
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
