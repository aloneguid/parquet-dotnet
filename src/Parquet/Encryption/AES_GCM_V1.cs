using System;
using System.IO;
using System.Linq;
using Parquet.Meta.Proto;
using System.Security.Cryptography;

namespace Parquet.Encryption {
    /// <summary>
    /// AES-GCM v1 decryption for all Parquet modular-encryption modules.
    /// Module framing (per spec):
    ///   length (4 LE) | nonce (12) | ciphertext | tag (16)
    /// AAD:
    ///   aad = AadPrefix || (AadFileUnique || moduleId || [rowGroupLE16] || [columnLE16] || [pageLE16])
    /// </summary>
    internal class AES_GCM_V1_Encryption : EncryptionBase {
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
            short? pageOrdinal = null) {

            if(SecretKey == null || SecretKey.Length == 0) {
                throw new InvalidDataException("Missing decryption key for AES-GCM-V1 decryption.");
            }

            // Read total encrypted buffer length(little-endian)
            byte[] lenBytes = ReadExactlyOrInvalid(reader, 4, "GCM length").EnsureLittleEndian();
            int totalLength = BitConverter.ToInt32(lenBytes, 0);
            if(totalLength < NonceLength + TagLength) {
                throw new InvalidDataException("Encrypted buffer too small for AES-GCM framing.");
            }

            // Read nonce
            byte[] nonce = ReadExactlyOrInvalid(reader, NonceLength, "GCM nonce");

            // Read ciphertext and tag
            int cipherTextLength = totalLength - NonceLength - TagLength;
            if(cipherTextLength < 0) {
                throw new InvalidDataException("Invalid ciphertext length for AES-GCM framing.");
            }

            byte[] cipherText = ReadExactlyOrInvalid(reader, cipherTextLength, "GCM ciphertext");
            byte[] tag = ReadExactlyOrInvalid(reader, TagLength, "GCM tag");

            // Build AAD = AadPrefix || (AadFileUnique || moduleId || [ordinals])
            byte[] prefix = AadPrefix ?? Array.Empty<byte>();
            byte[] fileUnique = AadFileUnique ?? throw new InvalidDataException("Missing AadFileUnique for AES-GCM-V1 decryption.");
            byte[] aadSuffix = BuildAadSuffix(fileUnique, module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
            byte[] aad = prefix.Concat(aadSuffix).ToArray();

#if NET8_0_OR_GREATER
        using var cipher = new AesGcm(SecretKey, TagLength);
        byte[] plainText = new byte[cipherTextLength];
        cipher.Decrypt(nonce, cipherText, tag, plainText, aad);
        return plainText;
#else
            throw new PlatformNotSupportedException("AES-GCM decryption requires netcoreapp3.0+ (e.g., .NET 5/6/7/8).");
#endif
        }

        private static byte[] BuildAadSuffix(
            byte[] aadFileUnique,
            Meta.ParquetModules module,
            short? rowGroupOrdinal,
            short? columnOrdinal,
            short? pageOrdinal) {

            // AAD suffix = AadFileUnique
            //            + moduleId (1 byte)
            //            + [rowGroupOrdinal LE16]
            //            + [columnOrdinal LE16]
            //            + [pageOrdinal LE16]
            using var ms = new MemoryStream();
            ms.Write(aadFileUnique, 0, aadFileUnique.Length);
            ms.WriteByte((byte)module);

            if(rowGroupOrdinal.HasValue) {
                ms.Write(BitConverter.GetBytes(rowGroupOrdinal.Value).EnsureLittleEndian(), 0, sizeof(short));
            }

            if(columnOrdinal.HasValue) {
                ms.Write(BitConverter.GetBytes(columnOrdinal.Value).EnsureLittleEndian(), 0, sizeof(short));
            }

            if(pageOrdinal.HasValue) {
                ms.Write(BitConverter.GetBytes(pageOrdinal.Value).EnsureLittleEndian(), 0, sizeof(short));
            }

            return ms.ToArray();
        }

        // ---- GCM framing helper: 4-byte length (LE) | nonce(12) | ciphertext | tag(16)
        private static byte[] FrameGcm(ReadOnlySpan<byte> nonce12, ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> tag16) {
            int len = nonce12.Length + ciphertext.Length + tag16.Length;
            byte[] framed = new byte[4 + len];
            BitConverter.GetBytes(len).CopyTo(framed, 0); // little-endian
            nonce12.CopyTo(framed.AsSpan(4));
            ciphertext.CopyTo(framed.AsSpan(4 + nonce12.Length));
            tag16.CopyTo(framed.AsSpan(4 + nonce12.Length + ciphertext.Length));
            return framed;
        }

        // Build full AAD = AadPrefix || BuildAadSuffix(...)
        private byte[] BuildAad(Meta.ParquetModules module, short? rowGroupOrdinal, short? columnOrdinal, short? pageOrdinal) {
            byte[] prefix = AadPrefix ?? Array.Empty<byte>();
            if(prefix.Length > 1_000_000) {
                throw new InvalidDataException("AAD prefix too large.");
            }
            byte[] fileUnique = AadFileUnique ?? throw new InvalidDataException("Missing AadFileUnique for AES-GCM-V1 encryption.");
            byte[] suffix = BuildAadSuffix(fileUnique, module, rowGroupOrdinal, columnOrdinal, pageOrdinal);

            byte[] aad = new byte[prefix.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, aad, 0, prefix.Length);
            Buffer.BlockCopy(suffix, 0, aad, prefix.Length, suffix.Length);
            return aad;
        }

        // Core GCM encrypt for any module
        internal byte[] EncryptModuleGcm(byte[] plaintext, Meta.ParquetModules module, short? rowGroupOrdinal = null, short? columnOrdinal = null, short? pageOrdinal = null) {
#if !NET8_0_OR_GREATER
            throw new PlatformNotSupportedException("AES-GCM encryption requires netcoreapp3.0+ (e.g., .NET 5/6/7/8).");
#else
    if (SecretKey == null || SecretKey.Length == 0)
        throw new InvalidDataException("Missing key for AES-GCM-V1 encryption.");

    byte[] nonce = RandomNumberGenerator.GetBytes(NonceLength);
    byte[] aad = BuildAad(module, rowGroupOrdinal, columnOrdinal, pageOrdinal);

    byte[] tag = new byte[TagLength];
    byte[] ct = new byte[plaintext.Length];

    using var gcm = new AesGcm(SecretKey, TagLength);
    gcm.Encrypt(nonce, plaintext, ct, tag, aad);

    return FrameGcm(nonce, ct, tag);
#endif
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

        public override byte[] EncryptColumnMetaData(byte[] bytes, short rowGroupOrdinal, short columnOrdinal)
            => EncryptModuleGcm(bytes, Meta.ParquetModules.ColumnMetaData, rowGroupOrdinal, columnOrdinal);

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
