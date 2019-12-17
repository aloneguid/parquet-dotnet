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
      public DataColumnStatistics(long nullCount, long distinctCount, object minValue, object maxValue)
      {
         NullCount = nullCount;
         DistinctCount = distinctCount;
         MinValue = minValue;
         MaxValue = maxValue;
      }

      /// <summary>
      /// Number of null values
      /// </summary>
      public long NullCount { get; }

      /// <summary>
      /// Number of distinct values
      /// </summary>
      public long DistinctCount { get; }

      /// <summary>
      /// Minimum value, casted to CLR type
      /// </summary>
      public object MinValue { get; }

      /// <summary>
      /// Maximum value, casted to CLR type
      /// </summary>
      public object MaxValue { get; }
   }
}
