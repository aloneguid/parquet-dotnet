namespace Parquet.Schema;

/// <summary>
/// Support Time/Timestamp Units
/// </summary>
public enum DateTimeTimeUnit {
    /// <summary>
    /// Millisecond Precision
    /// </summary>
    Millis,
    /// <summary>
    /// Microsecond Precision
    /// </summary>
    Micros,
    /// <summary>
    /// Nanosecond Precision, note dotnet does not support full Nano second precision for DateTime
    /// </summary>
    Nanos,
}