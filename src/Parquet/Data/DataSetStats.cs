using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
   class DataSetStats
   {
      private readonly DataSet _ds;
      private readonly Dictionary<SchemaElement, ColumnStats> _schemaToStats = new Dictionary<SchemaElement, ColumnStats>();

      public DataSetStats(DataSet ds)
      {
         _ds = ds;
      }

      public ColumnStats GetColumnStats(SchemaElement schema)
      {
         if (_schemaToStats.TryGetValue(schema, out ColumnStats result))
            return result;

         ColumnStats stats = CalculateStats(schema);
         _schemaToStats[schema] = stats;

         return stats;
      }

      private ColumnStats CalculateStats(SchemaElement schema)
      {
         int index = _ds.Schema.GetElementIndex(schema);
         var stats = new ColumnStats();

         foreach(object value in _ds.GetColumn(index))
         {
            if (value == null) stats.NullCount += 1;
         }

         return stats;
      }

   }
}
