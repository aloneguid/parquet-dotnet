using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet
{
   /// <summary>
   /// Represents data within parquet file
   /// </summary>
   public class ParquetDataSet
   {
      private readonly Dictionary<string, ParquetColumn> _columns = new Dictionary<string, ParquetColumn>();

      public ParquetDataSet()
      {

      }

      public ParquetDataSet(IEnumerable<ParquetColumn> columns)
      {
         foreach(ParquetColumn column in columns)
         {
            _columns.Add(column.Name, column);
         }
      }

      /// <summary>
      /// Gets dataset columns
      /// </summary>
      public List<ParquetColumn> Columns => new List<ParquetColumn>(_columns.Values);

      /// <summary>
      /// Gets column by name
      /// </summary>
      /// <param name="name"></param>
      /// <returns></returns>
      public ParquetColumn this[string name]
      {
         get
         {
            return _columns[name];
         }
      }

      /// <summary>
      /// Merges data into this dataset
      /// </summary>
      /// <param name="source"></param>
      public void Merge(ParquetDataSet source)
      {
         if (source == null) return;

         foreach(var kv in source._columns)
         {
            if(!_columns.TryGetValue(kv.Key, out ParquetColumn col))
            {
               _columns[kv.Key] = kv.Value;

               //todo: zero values
            }
            else
            {
               col.Add(kv.Value);
            }
         }
      }
   }
}
