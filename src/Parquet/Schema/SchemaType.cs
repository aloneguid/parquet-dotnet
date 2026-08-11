namespace Parquet.Schema;

/// <summary>
/// Type of schema
/// </summary>
public enum SchemaType {
    /// <summary>
    /// Contains actual values i.e. declared by a <see cref="DataField"/>
    /// </summary>
    Data,

    /// <summary>
    /// Contains dictionary definition
    /// </summary>
    Map,

    /// <summary>
    /// Contains structure definition
    /// </summary>
    Struct,

    /// <summary>
    /// Contains list definition
    /// </summary>
    List
}