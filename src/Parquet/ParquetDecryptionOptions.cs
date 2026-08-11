using System;
using System.Collections.Generic;

namespace Parquet;

/// <summary>
/// Configures key retrieval and AAD for encrypted Parquet files.
/// </summary>
public sealed class ParquetDecryptionOptions {
    /// <summary>
    /// Direct footer key. This takes precedence over <see cref="KeyRetriever"/>.
    /// </summary>
    public byte[]? FooterKey { get; set; }

    /// <summary>
    /// Direct column keys indexed by full physical column path formatted by
    /// <c>DataField.Path.ToString()</c>.
    /// </summary>
    public IDictionary<string, byte[]> ColumnKeys { get; } =
        new Dictionary<string, byte[]>(StringComparer.Ordinal);

    /// <summary>
    /// Optional resolver for keys identified by metadata in the file.
    /// </summary>
    public IParquetKeyRetriever? KeyRetriever { get; set; }

    /// <summary>
    /// Expected AAD prefix. For files that do not store their prefix, this value is also used for decryption.
    /// </summary>
    public byte[]? AadPrefix { get; set; }
}
