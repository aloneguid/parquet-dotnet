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

    /// <summary>
    /// Retrieves a raw AES key for a specific column, or for the footer when
    /// <paramref name="columnPath"/> is <see langword="null"/>.
    /// </summary>
    /// <param name="columnPath">
    /// Full physical column path formatted by <c>DataField.Path.ToString()</c>,
    /// or <see langword="null"/> when resolving the footer key.
    /// </param>
    /// <param name="keyMetadata">Opaque metadata supplied by the file writer.</param>
    /// <returns>A 16, 24, or 32 byte AES key.</returns>
    /// <remarks>
    /// The default implementation preserves compatibility with retrievers that
    /// only use key metadata.
    /// </remarks>
    ReadOnlyMemory<byte> GetKey(string? columnPath, ReadOnlyMemory<byte> keyMetadata) =>
        GetKey(keyMetadata);
}
