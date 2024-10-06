using System;
using Parquet.Schema;

namespace Parquet.Serialization.Attributes {


    /// <summary>
    /// Resolution of Parquet timestamp
    /// </summary>
    public enum ParquetTimestampResolution {
        /// <summary>
        /// Milliseconds, maps to <see cref="DateTimeFormat.DateAndTime"/>"
        /// </summary>
        Milliseconds,

#if NET7_0_OR_GREATER
        /// <summary>
        /// Microseconds, maps to <see cref="DateTimeFormat.DateAndTimeMicros"/>"
        /// </summary>
        Microseconds
#endif

        // nanoseconds to be added
    }

    /// <summary>
    /// Specifies that a property of type <see cref="DateTime"/> should be serialized as Parquet timestamp, which is internally
    /// an int64 number.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetTimestampAttribute : Attribute {

        /// <summary>
        /// Creates an instance of the attribute
        /// </summary>
        /// <param name="resolution"></param>
        /// <param name="useLogicalTimestamp"></param>
        /// <param name="isAdjustedToUTC"></param>
        public ParquetTimestampAttribute(ParquetTimestampResolution resolution = ParquetTimestampResolution.Milliseconds, bool useLogicalTimestamp = false, bool isAdjustedToUTC = true) {
            Resolution = resolution;
            UseLogicalTimestamp = useLogicalTimestamp;
            IsAdjustedToUTC = isAdjustedToUTC;
        }

        /// <summary>
        /// Resolution of Parquet timestamp
        /// </summary>
        public ParquetTimestampResolution Resolution { get; private set; }
        
        /// <summary>
        /// Resolution of Parquet timestamp
        /// </summary>
        public bool UseLogicalTimestamp { get; private set; }
        
        /// <summary>
        /// IsAdjustedToUTC
        /// </summary>
        public bool IsAdjustedToUTC { get; private set; }

        internal DateTimeFormat GetDateTimeFormat() {
            if(UseLogicalTimestamp)
                return DateTimeFormat.Timestamp;
            
            return Resolution switch {
                ParquetTimestampResolution.Milliseconds => DateTimeFormat.DateAndTime,
#if NET7_0_OR_GREATER
                ParquetTimestampResolution.Microseconds => DateTimeFormat.DateAndTimeMicros,
#endif
                _ => throw new NotSupportedException($"Timestamp resolution {Resolution} is not supported")
            };
        }
    }
}
