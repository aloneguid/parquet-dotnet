using System;

namespace Parquet;

/// <summary>
/// Resolves encryption keys from opaque metadata stored in a Parquet file.
/// </summary>
public interface IParquetKeyRetriever {
    /// <summary>
    /// Retrieves a raw AES key.
    /// </summary>
    /// <param name="keyMetadata">Opaque metadata supplied by the file writer.</param>
    /// <returns>A 16, 24, or 32 byte AES key.</returns>
    ReadOnlyMemory<byte> GetKey(ReadOnlyMemory<byte> keyMetadata);
}
