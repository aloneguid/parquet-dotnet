using System;

namespace Parquet.Schema;

/// <summary>
/// Represents TIME logical type, which is a time of day since midnight. See
/// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time
/// Maps to either <see cref="int"/> or <see cref="long"/> data type.
/// </summary>
public class TimeDataField : DataField {
    /// <summary>
    /// Time data field
    /// </summary>
    /// <param name="name"></param>
    /// <param name="precision"></param>
    /// <param name="isNullable"></param>
    /// <param name="isArray"></param>
    /// <param name="propertyName"></param>
    public TimeDataField(string name, TimeUnitPrecision precision, bool? isNullable = null, bool? isArray = null, string? propertyName = null)
        : base(name, GetUnitType(precision), isNullable, isArray, propertyName) =>
        Precision = precision;

    /// <summary>
    /// Indicates if the time is adjusted to UTC.
    /// </summary>
    public bool IsAdjustedToUtc { get; set; } = true;
    
    /// <summary>
    /// Time precision.
    /// </summary>
    public TimeUnitPrecision Precision { get; }
    
    private static Type GetUnitType(TimeUnitPrecision precision) =>
        precision switch {
            TimeUnitPrecision.Millis => typeof(int),
            TimeUnitPrecision.Micros or TimeUnitPrecision.Nanos => typeof(long),
            _ => throw new ArgumentOutOfRangeException(nameof(precision), precision, null)
        };
}