using System.Collections;

namespace Parquet.Data.Stats
{
   /// <summary>
   /// Contains the details for each of the coolumns where stats are calculated
   /// </summary>
   public struct ColumnStatsDetails
   {
      /// <summary>
      /// Used to construct a ColumnStartDetails
      /// </summary>
      public ColumnStatsDetails(IList values, ColumnSummaryStats columnSummaryStats, System.Type[] acceptedTypes,
         System.Type columnType)
      {
         Values = values;
         ColumnSummaryStats = columnSummaryStats;
         AcceptedTypes = acceptedTypes;
         ColumnType = columnType;
      }
      /// <summary>
      /// The stats values
      /// </summary>
      public IList Values;
      /// <summary>
      /// The type to hold the values
      /// </summary>
      public ColumnSummaryStats ColumnSummaryStats;
      /// <summary>
      /// The types that can be used to calculate the value
      /// </summary>
      public System.Type[] AcceptedTypes;
      /// <summary>
      /// The type of column for the schema 
      /// </summary>
      public System.Type ColumnType;
   }
}