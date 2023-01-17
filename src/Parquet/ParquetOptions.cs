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
        /// Whether to use dictionary encoding for string columns
        /// </summary>
        public bool UseDictionaryEncoding { get; set; } = false;

        /// <summary>
        /// Value from 0 to 1 indication threshold when string dictionary encoding is applied.
        /// For instance if there are exactly half distinct values, the threshold is 0.5.
        /// </summary>
        public double DictionaryEncodingThreshold { get; set; } = 0.8;

    }
}
