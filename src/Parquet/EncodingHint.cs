namespace Parquet;

/// <summary>
/// Encoding hint per column. This should be used from <see cref="ParquetOptions"/>
/// </summary>
public enum EncodingHint {
    /// <summary>
    /// Default encoding, chosen based on data type and other factors.
    /// </summary>
    Default = 0,

    /// <summary>
    /// Dictionary encoding, applies to strings and byte arrays.
    /// </summary>
    Dictionary,

    /// <summary>
    /// Delta binary packed encoding can be applied to INT32 and INT64 fields.
    /// </summary>
    DeltaBinaryPacked,

    /// <summary>
    /// Byte split stream encoding, supported FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY.
    /// </summary>
    ByteSplitStream
}
