using Parquet.Data;

namespace Parquet.Schema;

/// <summary>
/// Maps to Parquet decimal type, allowing to specify custom scale and precision.
/// </summary>
public class BigDecimalDataField : DecimalDataField {

    /// <summary>
    /// Constructs class instance
    /// </summary>
    /// <param name="name">The name of the column</param>
    /// <param name="precision">Custom precision</param>
    /// <param name="scale">Custom scale</param>
    /// <param name="isNullable"></param>
    /// <param name="isArray"></param>
    /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
    public BigDecimalDataField(string name, int precision, int scale = 0, bool? isNullable = null, bool? isArray = null, string? propertyName = null)
        : base(name, precision, scale, true, isNullable, isArray, propertyName, typeof(BigDecimal)) {
    }
}
