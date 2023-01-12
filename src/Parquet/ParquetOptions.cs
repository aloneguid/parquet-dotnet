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
        /// When true, date columns will be deserialized to <see cref="DateTime"/> instead of <see cref="DateTimeOffset"/>
        /// </summary>
        public bool PreferDateToDateTimeOffset { get; set; } = false;
    }
}
