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
    /// Dictionary encoding, applies to <see cref="string"/> and <see cref="byte"/>[] fields.
    /// </summary>
    Dictionary,

    /// <summary>
    /// Delta binary packed encoding can be applied to <see cref="int"/> and <see cref="long"/> fields.
    /// </summary>
    DeltaBinaryPacked,

    /// <summary>
    /// Byte split stream encoding. Not all of the types can be encoded. Currently supported types are:
    /// <see cref="float"/>, <see cref="double"/>, <see cref="int"/>, <see cref="long"/>, <see cref="short"/>,
    /// <see cref="ushort"/>,
    /// </summary>
    ByteSplitStream
}
