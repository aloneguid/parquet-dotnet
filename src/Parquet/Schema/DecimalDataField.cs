using System;

namespace Parquet.Schema {
    /// <summary>
    /// Maps to Parquet decimal type, allowing to specify custom scale and precision
    /// </summary>
    public class DecimalDataField : DataField {
        /// <summary>
        /// Precision
        /// </summary>
        public int Precision { get; }

        /// <summary>
        /// Scale
        /// </summary>
        public int Scale { get; }

        /// <summary>
        /// Gets a flag indicating whether byte array encoding is forced.
        /// </summary>
        public bool ForceByteArrayEncoding { get; }

        /// <summary>
        /// Constructs class instance
        /// </summary>
        /// <param name="name">The name of the column</param>
        /// <param name="precision">Custom precision</param>
        /// <param name="scale">Custom scale</param>
        /// <param name="forceByteArrayEncoding">Whether to force decimal type encoding as fixed bytes. Hive and Impala only understands decimals when forced to true.</param>
        /// <param name="hasNulls">Is 'decimal?'</param>
        /// <param name="isArray">Indicates whether this field is repeatable.</param>
        /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
        public DecimalDataField(string name, int precision, int scale = 0, bool forceByteArrayEncoding = false, bool hasNulls = true, bool isArray = false, string propertyName = null)
           : base(name, DataType.Decimal, hasNulls, isArray, propertyName) {
            // see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal for more details.
            if(precision < 1)
                throw new ArgumentException("precision is required and must be a non-zero positive integer", nameof(precision));
            if(scale < 0)
                throw new ArgumentException("scale must be zero or a positive integer", nameof(scale));
            if(scale >= precision)
                throw new ArgumentException("scale must be less than the precision", nameof(scale));

            Precision = precision;
            Scale = scale;
            ForceByteArrayEncoding = forceByteArrayEncoding;
        }
    }
}