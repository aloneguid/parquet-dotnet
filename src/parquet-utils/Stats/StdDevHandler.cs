using System;
using System.Collections;
using System.Linq;

namespace Parquet.Data.Stats
{
   /// <summary>
   /// Used to return the min value of the column
   /// </summary>
   public class StdDevHandler : StatsHandler
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
         double count = values.Values.Count;
         double sum = values.Values.Cast<object>().Sum(value => Convert.ToDouble(value));
         double average = sum / count;
         double varianceSum = values.Values.Cast<object>().Sum(item => Math.Pow(Convert.ToDouble(item) - average, 2));
         values.ColumnSummaryStats.StandardDeviation = Math.Sqrt(varianceSum / (count - 1));
         return values.ColumnSummaryStats;
      }
   }
}