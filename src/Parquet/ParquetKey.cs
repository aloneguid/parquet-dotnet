using System;

namespace Parquet;

/// <summary>
/// Contains an AES key and optional retrieval metadata stored in a Parquet file.
/// </summary>
public sealed class ParquetKey {
    /// <summary>
    /// Creates a key from raw AES key bytes.
    /// </summary>
    /// <param name="keyBytes">A 16, 24, or 32 byte AES key.</param>
    /// <param name="keyMetadata">Optional opaque metadata used to retrieve the key.</param>
    public ParquetKey(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> keyMetadata = default) {
        if(keyBytes.Length is not (16 or 24 or 32))
            throw new ArgumentException("AES keys must contain 16, 24, or 32 bytes.", nameof(keyBytes));

        KeyBytes = keyBytes.ToArray();
        KeyMetadata = keyMetadata.IsEmpty ? null : keyMetadata.ToArray();
    }

    /// <summary>
    /// Raw AES key bytes.
    /// </summary>
    public byte[] KeyBytes { get; }

    /// <summary>
    /// Optional opaque retrieval metadata written to the Parquet file.
    /// </summary>
    public byte[]? KeyMetadata { get; }
}
