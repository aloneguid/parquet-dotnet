using System.Collections;
using System.Linq;

namespace Parquet.Data.Stats
{
   /// <summary>
   /// Used to return the number of null values in the column
   /// </summary>
   public class NullStatsHandler : StatsHandler
   {
      /// <summary>
      /// Gets the count of null values given the list of column values
      /// </summary>
      /// <param name="values">A list of values</param>
      /// <returns>A count of null values</returns>
      public override ColumnSummaryStats GetColumnStats(ColumnStatsDetails values)
      {
         if (!CanCalculateWithType(values))
            return values.ColumnSummaryStats;
         int totalNulls = values.Values.Cast<object>().Count(value => value == null);
         values.ColumnSummaryStats.NullCount = totalNulls;
         return values.ColumnSummaryStats;
      }
   }
}