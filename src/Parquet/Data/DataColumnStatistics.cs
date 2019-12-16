namespace Parquet.Data
{
   /// <summary>
   /// Basic statistics for data column
   /// </summary>
   public class DataColumnStatistics
   {
      /// <summary>
      /// 
      /// </summary>
      public DataColumnStatistics(long nullCount, long distinctCount)
      {
         NullCount = nullCount;
         DistinctCount = distinctCount;
      }

      /// <summary>
      /// Number of null values
      /// </summary>
      public long NullCount { get; }

      /// <summary>
      /// Number of distinct values
      /// </summary>
      public long DistinctCount { get; }
   }
}
