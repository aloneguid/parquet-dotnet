using System;
using System.Collections.Generic;

namespace Parquet;

/// <summary>
/// Configures Parquet modular encryption for a writer.
/// </summary>
public sealed class ParquetEncryptionOptions {
    /// <summary>
    /// Creates encryption options using the supplied footer key.
    /// </summary>
    public ParquetEncryptionOptions(ParquetKey footerKey) {
        FooterKey = footerKey ?? throw new ArgumentNullException(nameof(footerKey));
    }

    /// <summary>
    /// Key used to encrypt or sign the footer and, when selected, column modules.
    /// </summary>
    public ParquetKey FooterKey { get; }

    /// <summary>
    /// Encryption algorithm. The default is AES-GCM.
    /// </summary>
    public ParquetEncryptionAlgorithm Algorithm { get; set; } = ParquetEncryptionAlgorithm.AesGcmV1;

    /// <summary>
    /// Whether to encrypt the footer. When false, the plaintext footer is signed.
    /// </summary>
    public bool EncryptFooter { get; set; } = true;

    /// <summary>
    /// Whether every column not listed in <see cref="ColumnKeys"/> is encrypted with the footer key.
    /// </summary>
    public bool EncryptAllColumns { get; set; } = true;

    /// <summary>
    /// Columns encrypted with the footer key when <see cref="EncryptAllColumns"/> is false.
    /// Values are full physical column paths formatted by <c>DataField.Path.ToString()</c>.
    /// </summary>
    public ISet<string> FooterKeyColumns { get; } = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// Column-specific keys indexed by full physical column path formatted by
    /// <c>DataField.Path.ToString()</c>.
    /// </summary>
    public IDictionary<string, ParquetKey> ColumnKeys { get; } =
        new Dictionary<string, ParquetKey>(StringComparer.Ordinal);

    /// <summary>
    /// Optional additional authenticated data prefix identifying this file.
    /// </summary>
    public byte[]? AadPrefix { get; set; }

    /// <summary>
    /// Whether <see cref="AadPrefix"/> is stored in the file. When false, readers must supply it.
    /// </summary>
    public bool StoreAadPrefix { get; set; } = true;
}
