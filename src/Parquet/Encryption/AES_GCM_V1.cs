using System;
using System.IO;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
    /// <summary>
    /// AES-GCM v1 decryption for all Parquet modular-encryption modules.
    /// Module framing (per spec):
    ///   length (4 LE) | nonce (12) | ciphertext | tag (16)
    /// AAD:
    ///   aad = AadPrefix || (AadFileUnique || moduleId || [rowGroupLE16] || [columnLE16] || [pageLE16])
    /// </summary>
    internal class AES_GCM_V1_Encryption : EncryptionBase {
        // sane default max (64MB) - protects against OOM attacks
        const int MaxEncryptedModule = 64 * 1024 * 1024;

        private const int NonceLength = 12;
        private const int TagLength = 16;

        public AES_GCM_V1_Encryption() { }

        public override byte[] BloomFilterBitset(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal)
            => Decrypt(reader, Meta.ParquetModules.BloomFilter_Bitset, rowGroupOrdinal, columnOrdinal);

        public override byte[] BloomFilterHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal)
            => Decrypt(reader, Meta.ParquetModules.BloomFilter_Header, rowGroupOrdinal, columnOrdinal);

        public override byte[] DecryptColumnIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal)
            => Decrypt(reader, Meta.ParquetModules.ColumnIndex, rowGroupOrdinal, columnOrdinal);

        public override byte[] DecryptColumnMetaData(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal)
            => Decrypt(reader, Meta.ParquetModules.ColumnMetaData, rowGroupOrdinal, columnOrdinal);

        public override byte[] DecryptDataPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal)
            => Decrypt(reader, Meta.ParquetModules.Data_Page, rowGroupOrdinal, columnOrdinal, pageOrdinal);

        public override byte[] DecryptDataPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal)
            => Decrypt(reader, Meta.ParquetModules.Data_PageHeader, rowGroupOrdinal, columnOrdinal, pageOrdinal);

        public override byte[] DecryptDictionaryPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal)
            => Decrypt(reader, Meta.ParquetModules.Dictionary_Page, rowGroupOrdinal, columnOrdinal);

        public override byte[] DecryptDictionaryPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal)
            => Decrypt(reader, Meta.ParquetModules.Dictionary_PageHeader, rowGroupOrdinal, columnOrdinal);

        public override byte[] DecryptFooter(ThriftCompactProtocolReader reader)
            => Decrypt(reader, Meta.ParquetModules.Footer);

        public override byte[] DecryptOffsetIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal)
            => Decrypt(reader, Meta.ParquetModules.OffsetIndex, rowGroupOrdinal, columnOrdinal);

        /// <summary>
        /// Shared AES-GCM decrypt path for all modules.
        /// Framing: length(4 LE) | nonce(12) | ciphertext | tag(16)
        /// </summary>
        protected virtual byte[] Decrypt(
            ThriftCompactProtocolReader reader,
            Meta.ParquetModules module,
            short? rowGroupOrdinal = null,
            short? columnOrdinal = null,
            short? pageOrdinal = null
        ) {
            if(FooterEncryptionKey is null || FooterEncryptionKey.Length == 0)
                throw new InvalidDataException("Missing decryption key for AES-GCM-V1 decryption.");

            // Spec framing: length(4 LE) | nonce(12) | ciphertext | tag(16)
            byte[] lenBytes = ReadExactlyOrInvalid(reader, 4, "GCM length").EnsureLittleEndian();
            int totalLength = BitConverter.ToInt32(lenBytes, 0);

            const int NonceLen = 12;
            const int TagLen = 16;

            if(totalLength < NonceLen + TagLen)
                throw new InvalidDataException($"Encrypted GCM buffer too small ({totalLength} bytes).");

            byte[] nonce = ReadExactlyOrInvalid(reader, NonceLen, "GCM nonce");
            int ctLen = totalLength - NonceLen - TagLen;
            if(ctLen < 0)
                throw new InvalidDataException("Invalid ciphertext length for AES-GCM framing.");

            byte[] ct = ReadExactlyOrInvalid(reader, ctLen, "GCM ciphertext");
            byte[] tag = ReadExactlyOrInvalid(reader, TagLen, "GCM tag");

            // AAD = prefix || suffix(file-unique, module, ordinals)
            byte[] aad = BuildAad(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);

            // Decrypt
            byte[] pt = new byte[ctLen];
            CryptoHelpers.GcmDecryptOrThrow(FooterEncryptionKey!, nonce, ct, tag, pt, aad);

            return pt;
        }

        // Core GCM encrypt for any module
        internal byte[] EncryptModuleGcm(
            byte[] plaintext,
            Meta.ParquetModules module,
            short? rowGroupOrdinal = null,
            short? columnOrdinal = null,
            short? pageOrdinal = null
        ) {
            if(FooterEncryptionKey == null || FooterEncryptionKey.Length == 0)
                throw new InvalidDataException("Missing key for AES-GCM-V1 encryption.");

            byte[] nonce12 = CryptoHelpers.GetRandomBytes(12);
            byte[] aad = BuildAad(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);

            byte[] tag = new byte[TagLength];
            byte[] ct = new byte[plaintext.Length];

            CryptoHelpers.GcmEncryptOrThrow(FooterEncryptionKey!, nonce12, plaintext, ct, tag, aad);
            CountGcmInvocation();

            return FrameGcm(nonce12, ct, tag);
        }

        // ---- Per-module convenience wrappers (match your decryptors) ----

        public override byte[] EncryptFooter(byte[] plaintext)
            => EncryptModuleGcm(plaintext, Meta.ParquetModules.Footer);

        public override byte[] EncryptDictionaryPageHeader(byte[] header, short rowGroupOrdinal, short columnOrdinal)
            => EncryptModuleGcm(header, Meta.ParquetModules.Dictionary_PageHeader, rowGroupOrdinal, columnOrdinal);

        public override byte[] EncryptDataPageHeader(byte[] header, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal)
            => EncryptModuleGcm(header, Meta.ParquetModules.Data_PageHeader, rowGroupOrdinal, columnOrdinal, pageOrdinal);

        public override byte[] EncryptDataPage(byte[] header, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal)
            => EncryptModuleGcm(header, Meta.ParquetModules.Data_Page, rowGroupOrdinal, columnOrdinal, pageOrdinal);

        public override byte[] EncryptDictionaryPage(byte[] body, short rowGroupOrdinal, short columnOrdinal)
            => EncryptModuleGcm(body, Meta.ParquetModules.Dictionary_Page, rowGroupOrdinal, columnOrdinal);

        public override byte[] EncryptColumnMetaDataWithKey(byte[] plain, short rg, short col, byte[] key) {
            byte[]? saved = FooterEncryptionKey;
            try {
                FooterEncryptionKey = key;
                return EncryptModuleGcm(plain, Meta.ParquetModules.ColumnMetaData, rg, col);
            } finally {
                FooterEncryptionKey = saved;
            }
        }

        public override byte[] EncryptColumnIndex(byte[] bytes, short rowGroupOrdinal, short columnOrdinal)
            => EncryptModuleGcm(bytes, Meta.ParquetModules.ColumnIndex, rowGroupOrdinal, columnOrdinal);

        public override byte[] EncryptOffsetIndex(byte[] bytes, short rowGroupOrdinal, short columnOrdinal)
            => EncryptModuleGcm(bytes, Meta.ParquetModules.OffsetIndex, rowGroupOrdinal, columnOrdinal);

        public override byte[] EncryptBloomFilterHeader(byte[] bytes, short rowGroupOrdinal, short columnOrdinal)
            => EncryptModuleGcm(bytes, Meta.ParquetModules.BloomFilter_Header, rowGroupOrdinal, columnOrdinal);

        public override byte[] EncryptBloomFilterBitset(byte[] bytes, short rowGroupOrdinal, short columnOrdinal)
            => EncryptModuleGcm(bytes, Meta.ParquetModules.BloomFilter_Bitset, rowGroupOrdinal, columnOrdinal);
    }
}
