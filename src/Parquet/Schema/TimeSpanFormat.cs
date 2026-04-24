namespace Parquet.Schema;

/// <summary>
/// Choice of representing time
/// </summary>
public enum TimeSpanFormat {
    /// <summary>
    /// A time
    /// 
    /// The total number of milliseconds since midnight.  The value is stored
    /// as an INT32 physical type.
    /// </summary>
    MilliSeconds,

    /// <summary>
    /// A time.
    /// 
    /// The total number of microseconds since midnight.  The value is stored as
    /// an INT64 physical type.
    /// </summary>
    MicroSeconds
}