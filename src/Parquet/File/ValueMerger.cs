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
      public IList Apply(IList dictionary, List<int> definitions, List<int> indexes, int maxValues)
      {
         if (dictionary == null && definitions == null && indexes == null) return _values;  //values are just values

         ApplyDictionary(dictionary, indexes, maxValues);

         ApplyDefinitions(definitions, maxValues);

         return _values;
      }

      private void ApplyDictionary(IList dictionary, List<int> indexes, int maxValues)
      {
         //merge with dictionary if present
         if (dictionary == null) return;

         if (indexes == null) throw new ParquetException("dictionary has no attached index");

         TrimTail(indexes, maxValues);

         IList values = indexes
            .Select(i => dictionary[i])
            .ToList();

         TrimTail(values, maxValues);

         foreach (var el in values) _values.Add(el);
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

         TrimTail(values, maxValues);
         _values = values;
      }

      public static void TrimTail(IList list, int maxValues)
      {
         if (list.Count > maxValues)
         {
            int diffCount = list.Count - maxValues;
            while (--diffCount >= 0) list.RemoveAt(list.Count - 1); //more effective than copying the list again
         }
      }

      public static void TrimHead(IList list, int maxValues)
      {
         while(list.Count > maxValues)
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
