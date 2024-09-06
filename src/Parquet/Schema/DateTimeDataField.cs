using System;

namespace Parquet.Schema {
    /// <summary>
    /// Schema element for <see cref="DateTime"/> which allows to specify precision
    /// </summary>
    public class DateTimeDataField : DataField {
        /// <summary>
        /// Desired data format, Parquet specific
        /// </summary>
        public DateTimeFormat DateTimeFormat { get; }
        
        /// <summary>
        /// IsAdjustedToUTC
        /// </summary>
        public bool IsAdjustedToUTC { get; }
        
        /// <summary>
        /// TimeUnit
        /// </summary>
        public DateTimeTimeUnit Unit { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DateTimeDataField"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="format">The format.</param>
        /// <param name="isAdjustedToUTC"></param>
        /// <param name="unit"></param>
        /// <param name="isNullable"></param>
        /// <param name="isArray"></param>
        /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
        public DateTimeDataField(string name, DateTimeFormat format, bool isAdjustedToUTC = true, DateTimeTimeUnit? unit = null, bool? isNullable = null, bool? isArray = null, string? propertyName = null)
           : base(name, typeof(DateTime), isNullable, isArray, propertyName) {
            DateTimeFormat = format;
            IsAdjustedToUTC = isAdjustedToUTC;
            
            // Override the unit for legacy types
            if(format == DateTimeFormat.DateAndTime) {
                Unit = DateTimeTimeUnit.Millis;
            }
#if NET7_0_OR_GREATER
            else if(format == DateTimeFormat.DateAndTimeMicros) {
                Unit = DateTimeTimeUnit.Micros;
            }
#endif
            else {
                Unit = unit ?? DateTimeTimeUnit.Millis;
            }
        }
    }
}