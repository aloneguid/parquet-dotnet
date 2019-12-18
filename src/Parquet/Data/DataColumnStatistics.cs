namespace Parquet.Data
{
   /// <summary>
   /// Basic statistics for data column
   /// </summary>
   public class DataColumnStatistics
   {
      /// <summary>
      /// Creates an uninitalised instance of column statistics
      /// </summary>
      public DataColumnStatistics()
      {

      }

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
      public long NullCount { get; internal set; }

      /// <summary>
      /// Number of distinct values
      /// </summary>
      public long DistinctCount { get; internal set; }

      /// <summary>
      /// Minimum value, casted to CLR type
      /// </summary>
      public object MinValue { get; internal set; }

      /// <summary>
      /// Maximum value, casted to CLR type
      /// </summary>
      public object MaxValue { get; internal set; }

      internal Thrift.Statistics ToThriftStatistics(IDataTypeHandler handler, Thrift.SchemaElement tse)
      {
         byte[] min = handler.PlainEncode(tse, MinValue);
         byte[] max = handler.PlainEncode(tse, MaxValue);

         return new Thrift.Statistics
         {
            Null_count = NullCount,
            Distinct_count = DistinctCount,
            Min = min,
            Min_value = min,
            Max = max,
            Max_value = max
         };
      }
   }
}
