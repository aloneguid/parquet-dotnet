using System;

namespace Parquet {
    /// <summary>
    /// Parquet options
    /// </summary>
    public class ParquetOptions {
        /// <summary>
        /// When true byte arrays will be treated as UTF-8 strings
        /// </summary>
        public bool TreatByteArrayAsString { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether big integers are always treated as dates
        /// </summary>
        public bool TreatBigIntegersAsDates { get; set; } = true;

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

    }
}
