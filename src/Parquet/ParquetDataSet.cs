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
      private readonly Dictionary<ParquetColumn, ParquetColumn> _columns = new Dictionary<ParquetColumn, ParquetColumn>();

      public ParquetDataSet()
      {

      }

      public ParquetDataSet(IEnumerable<ParquetColumn> columns)
      {
         foreach(ParquetColumn column in columns)
         {
            _columns.Add(column, column);
         }
      }

      /// <summary>
      /// Gets dataset columns
      /// </summary>
      public List<ParquetColumn> Columns => new List<ParquetColumn>(_columns.Keys);

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
               _columns[kv.Key] = kv.Key;

               //todo: zero values
            }
            else
            {
               col.Add(kv.Key);
            }
         }
      }
   }
}
