using System;
using System.Collections.Generic;
using Parquet.Meta;

namespace Parquet {
    /// <summary>
    /// Parquet options
    /// </summary>
    public class ParquetOptions {
        /// <summary>
        /// When true byte arrays will be treated as UTF-8 strings on read
        /// </summary>
        public bool TreatByteArrayAsString { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether big integers are always treated as dates on read
        /// </summary>
        public bool TreatBigIntegersAsDates { get; set; } = true;

#if NET6_0_OR_GREATER
        /// <summary>
        /// When set to true, parquet dates will be deserialized as <see cref="DateOnly"/>, otherwise
        /// as <see cref="DateTime"/> with missing time part.
        /// </summary>
        public bool UseDateOnlyTypeForDates { get; set; } = false;

        /// <summary>
        /// When set to true, parquet times with millisecond precision will be deserialized as <see cref="TimeOnly"/>, otherwise
        /// as <see cref="TimeSpan"/> with missing time part.
        /// </summary>
        public bool UseTimeOnlyTypeForTimeMillis { get; set; } = false;

        /// <summary>
        /// When set to true, parquet times with microsecond precision will be deserialized as <see cref="TimeOnly"/>, otherwise
        /// as <see cref="TimeSpan"/> with missing time part.
        /// </summary>
        public bool UseTimeOnlyTypeForTimeMicros { get; set; } = false;
#endif

        /// <summary>
        /// Whether to use dictionary encoding for string columns. Other column types are not supported.
        /// </summary>
        public bool UseDictionaryEncoding { get; set; } = true;

        /// <summary>
        /// String dictionary uniqueness threshold, which is a value from 0 (no unique values) 
        /// to 1 (all values are unique) indicating when string dictionary encoding is applied.
        /// Uniqueness factor needs to be less or equal than this threshold.
        /// </summary>
        public double DictionaryEncodingThreshold { get; set; } = 0.8;

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, Encoding> ColumnEncoding { get; set; } = new Dictionary<string, Encoding>();

    }
}
