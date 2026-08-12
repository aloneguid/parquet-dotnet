using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.File;

namespace Parquet;

/// <summary>
/// Provides operations that update a Parquet file footer without rewriting its row groups.
/// </summary>
public static class ParquetFooter {
    /// <summary>
    /// Adds or replaces custom key-value metadata in a Parquet file.
    /// Existing entries whose keys are not present in <paramref name="metadata"/> are preserved.
    /// Encrypted and signed plaintext footers are authenticated before they are rewritten.
    /// </summary>
    /// <param name="filePath">Path to the Parquet file.</param>
    /// <param name="metadata">Custom metadata entries to add or replace.</param>
    /// <param name="parquetOptions">Optional decryption key and AAD settings.</param>
    /// <param name="cancellationToken">Token used to cancel footer authentication.</param>
    public static async Task UpdateCustomMetadataAsync(
        string filePath,
        IEnumerable<KeyValuePair<string, string>> metadata,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default) {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await using var stream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
        await UpdateCustomMetadataAsync(stream, metadata, parquetOptions, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Adds or replaces custom key-value metadata in a Parquet stream.
    /// Existing entries whose keys are not present in <paramref name="metadata"/> are preserved.
    /// Encrypted and signed plaintext footers are authenticated before they are rewritten.
    /// </summary>
    /// <param name="parquetStream">Readable, writable, seekable Parquet stream.</param>
    /// <param name="metadata">Custom metadata entries to add or replace.</param>
    /// <param name="parquetOptions">Optional decryption key and AAD settings.</param>
    /// <param name="cancellationToken">Token used to cancel footer authentication.</param>
    public static Task UpdateCustomMetadataAsync(
        Stream parquetStream,
        IEnumerable<KeyValuePair<string, string>> metadata,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(parquetStream);
        ArgumentNullException.ThrowIfNull(metadata);
        if(!parquetStream.CanRead || !parquetStream.CanWrite || !parquetStream.CanSeek) {
            throw new ArgumentException(
                "stream must be readable, writable, and seekable",
                nameof(parquetStream));
        }

        Dictionary<string, string> updates = MaterializeMetadata(metadata);
        return new FooterEditor(parquetStream).UpdateAsync(
            updates,
            parquetOptions ?? new ParquetOptions(),
            cancellationToken);
    }

    private static Dictionary<string, string> MaterializeMetadata(
        IEnumerable<KeyValuePair<string, string>> metadata) {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach(KeyValuePair<string, string> entry in metadata) {
            ArgumentNullException.ThrowIfNull(entry.Key);
            ArgumentNullException.ThrowIfNull(entry.Value);
            result[entry.Key] = entry.Value;
        }
        return result;
    }

    private sealed class FooterEditor(Stream stream) : ParquetActor(stream) {
        public async Task UpdateAsync(
            IReadOnlyDictionary<string, string> updates,
            ParquetOptions options,
            CancellationToken cancellationToken) {
            await ValidateFileAsync().ConfigureAwait(false);
            ParquetMetadataReadResult result = await ReadMetadataAsync(options, cancellationToken)
                .ConfigureAwait(false);
            var footer = new ThriftFooter(result.Metadata);
            Dictionary<string, string> customMetadata = footer.CustomMetadata;
            foreach(KeyValuePair<string, string> entry in updates)
                customMetadata[entry.Key] = entry.Value;
            footer.CustomMetadata = customMetadata;

            byte[] footerBytes = ParquetFooterWriter.Serialize(footer, result.CryptoContext);
            await GoBeforeFooterAsync().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            long footerStart = Stream.Position;
            Stream.SetLength(footerStart);
            await Stream.WriteAsync(footerBytes, CancellationToken.None).ConfigureAwait(false);
            await Stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
