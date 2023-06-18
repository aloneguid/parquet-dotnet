using System;

#if NET6_0_OR_GREATER
namespace Parquet.Schema {
    /// <summary>
    /// Schema element for <see cref="TimeOnly"/> which allows to specify precision
    /// </summary>
    public class TimeOnlyDataField : DataField {
        /// <summary>
        /// Desired data format, Parquet specific
        /// </summary>
        public TimeSpanFormat TimeSpanFormat { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TimeOnlyDataField"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="format">The format.</param>
        /// <param name="isNullable"></param>
        /// <param name="isArray"></param>
        /// <param name="propertyName"></param>
        public TimeOnlyDataField(string name, TimeSpanFormat format, bool? isNullable = null, bool? isArray = null, string? propertyName = null)
           : base(name, typeof(TimeOnly), isNullable, isArray, propertyName) {
            TimeSpanFormat = format;
        }
    }
}
#endif