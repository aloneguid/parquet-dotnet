using System;

namespace Parquet.Schema;

/// <summary>
/// Represents TIME logical type, which is a time of day since midnight.
/// </summary>
public class TimeDataField : DataField {

    /// <summary>
    /// Time precision. It's called "Unit" to match Parquet convention.
    /// </summary>
    public enum Unit {
        /// <summary>
        /// Milliseconds precision
        /// </summary>
        Millis,
        
        /// <summary>
        /// Microseconds precision
        /// </summary>
        Micros,
        
        /// <summary>
        /// Nanoseconds precision
        /// </summary>
        Nanos
    }
    
    /// <summary>
    /// Time data field
    /// </summary>
    /// <param name="name"></param>
    /// <param name="precision"></param>
    /// <param name="isNullable"></param>
    /// <param name="isArray"></param>
    /// <param name="propertyName"></param>
    public TimeDataField(string name, Unit precision, bool? isNullable = null, bool? isArray = null, string? propertyName = null)
        : base(name, GetUnitType(precision), isNullable, isArray, propertyName) =>
        Precision = precision;

    /// <summary>
    /// Indicates if the time is adjusted to UTC.
    /// </summary>
    public bool IsAdjustedToUtc { get; set; } = true;
    
    /// <summary>
    /// Time precision.
    /// </summary>
    public Unit Precision { get; init; }
    
    private static Type GetUnitType(Unit precision) =>
        precision switch {
            Unit.Millis => typeof(int),
            Unit.Micros or Unit.Nanos => typeof(long),
            _ => throw new ArgumentOutOfRangeException(nameof(precision), precision, null)
        };
}