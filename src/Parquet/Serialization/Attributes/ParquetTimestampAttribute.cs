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
        public ParquetTimestampAttribute(ParquetTimestampResolution resolution = ParquetTimestampResolution.Milliseconds) {
            Resolution = resolution;
        }

        /// <summary>
        /// Resolution of Parquet timestamp
        /// </summary>
        public ParquetTimestampResolution Resolution { get; private set; }

        internal DateTimeFormat GetDateTimeFormat() {
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
