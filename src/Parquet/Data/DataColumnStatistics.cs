using System;
using Parquet.Encodings;

namespace Parquet.Data {
    /// <summary>
    /// Basic statistics for data column
    /// </summary>
    public class DataColumnStatistics {
        /// <summary>
        /// Creates an uninitalised instance of column statistics
        /// </summary>
        public DataColumnStatistics() {

        }

        /// <summary>
        /// 
        /// </summary>
        public DataColumnStatistics(long? nullCount, long? distinctCount, object? minValue, object? maxValue) {
            NullCount = nullCount;
            DistinctCount = distinctCount;
            MinValue = minValue;
            MaxValue = maxValue;
        }

        /// <summary>
        /// Number of null values
        /// </summary>
        public long? NullCount { get; internal set; }

        /// <summary>
        /// Number of distinct values if set.
        /// </summary>
        public long? DistinctCount { get; internal set; }

        /// <summary>
        /// Minimum value, casted to CLR type
        /// </summary>
        public object? MinValue { get; internal set; }

        /// <summary>
        /// Maximum value, casted to CLR type
        /// </summary>
        public object? MaxValue { get; internal set; }

        internal Thrift.Statistics ToThriftStatistics(Thrift.SchemaElement tse) {

            if(!ParquetPlainEncoder.TryEncode(MinValue, tse, out byte[]? min)) {
                throw new ArgumentException($"cound not encode {MinValue}", nameof(MinValue));
            }

            if(!ParquetPlainEncoder.TryEncode(MaxValue, tse, out byte[]? max)) {
                throw new ArgumentException($"cound not encode {MinValue}", nameof(MinValue));
            }

            var r = new Thrift.Statistics();
            if(NullCount != null)
                r.Null_count = NullCount.Value;
            if(DistinctCount != null)
                r.Distinct_count = DistinctCount.Value;
            if(min != null)
                r.Min = r.Min_value = min;
            if(max != null)
                r.Max = r.Max_value = max;

            return r;
        }
    }
}
