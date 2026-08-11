namespace Parquet.Schema;

/// <summary>
/// Time precision. It's called "Unit" to match Parquet convention.
/// </summary>
public enum TimeUnitPrecision {
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