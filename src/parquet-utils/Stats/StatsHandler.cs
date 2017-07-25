using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.Thrift;

namespace Parquet.Data.Stats
{
   /// <summary>
   /// Handles all requests to get stats for the column
   /// </summary>
    public abstract class StatsHandler
    {
      /// <summary>
      /// Gets the stats for each column
      /// </summary>
      /// <param name="values">The values for each column</param>
      /// <returns>A columnstats type</returns>
       public abstract ColumnSummaryStats GetColumnStats(ColumnStatsDetails values);

      /// <summary>
      /// Determines whether this can be used with the apparent schema type
      /// </summary>
      /// <param name="values">The structure containing the values</param>
      /// <returns>A boolean value as to whether it can be calculated or not</returns>
      public bool CanCalculateWithType(ColumnStatsDetails values)
      {
         return values.AcceptedTypes.Contains(values.ColumnType);
      }
   }
}
