using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Specifies that a property represents a time interval to be stored in Parquet format, with optional adjustment to
    /// UTC.
    /// </summary>

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetTimeSpanAttribute : Attribute {
        /// <summary>
        /// IsAdjustedToUTC
        /// </summary>
        public bool IsAdjustedToUTC { get; set; } = true;
    }
}
