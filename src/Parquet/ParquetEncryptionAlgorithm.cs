namespace Parquet;

/// <summary>
/// Encryption algorithms defined by the Parquet modular encryption specification.
/// </summary>
public enum ParquetEncryptionAlgorithm {
    /// <summary>
    /// Encrypts every module with AES-GCM.
    /// </summary>
    AesGcmV1,

    /// <summary>
    /// Encrypts page bodies with AES-CTR and all other modules with AES-GCM.
    /// </summary>
    AesGcmCtrV1
}
