using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Parquet.Meta.Proto;
using Parquet.Meta;
using Encoding = System.Text.Encoding;
using System.Buffers.Binary;

namespace Parquet.Encryption {
    internal abstract class EncryptionBase {
        private const uint MaxInvocations = 0xFFFF_FFFF; // 2^32 - per NIST text
        private const long MaxPagesPerKey = 1L << 31;    // Parquet guidance (~2 billion)

        private long _gcmInvocations;
        private long _pageEncryptions; // data + dictionary pages

        internal byte[]? AadPrefix { get; set; }
        internal byte[]? FooterEncryptionKey { get; set; }
        internal byte[]? AadFileUnique { get; set; }

        public static byte[] DecryptFooter(
            ThriftCompactProtocolReader reader,
            string footerEncryptionKey,
            string? aadPrefix,
            out EncryptionBase decrypter) {
            if(string.IsNullOrEmpty(footerEncryptionKey)) {
                throw new ArgumentException($"Encrypted parquet files require an {nameof(footerEncryptionKey)} value");
            }

            var cryptoMetaData = FileCryptoMetaData.Read(reader);
            if(cryptoMetaData.EncryptionAlgorithm.AESGCMV1 is not null) {
                decrypter = new AES_GCM_V1_Encryption();
                decrypter.AadFileUnique = cryptoMetaData.EncryptionAlgorithm.AESGCMV1.AadFileUnique ?? Array.Empty<byte>();
                if(cryptoMetaData.EncryptionAlgorithm.AESGCMV1.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix)) {
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    }
                    decrypter.AadPrefix = Encoding.UTF8.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = cryptoMetaData.EncryptionAlgorithm.AESGCMV1.AadPrefix ?? Array.Empty<byte>();
                }
            } else if(cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1 is not null) {
                decrypter = new AES_GCM_CTR_V1_Encryption();

                AesGcmCtrV1 alg = cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1;
                decrypter.AadFileUnique = alg.AadFileUnique
                    ?? throw new InvalidDataException("Encrypted file is missing aad_file_unique.");

                if(alg.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix))
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    decrypter.AadPrefix = Encoding.UTF8.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = alg.AadPrefix ?? Array.Empty<byte>();
                }
            } else {
                throw new NotSupportedException("No encryption algorithm defined");
            }

            decrypter.FooterEncryptionKey = ParseKeyString(footerEncryptionKey);
            return decrypter.DecryptFooter(reader);
        }

        public abstract byte[] DecryptFooter(ThriftCompactProtocolReader reader);
        public abstract byte[] DecryptColumnMetaData(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptDataPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] DecryptDictionaryPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptDataPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] DecryptDictionaryPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptColumnIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptOffsetIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] BloomFilterHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] BloomFilterBitset(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);

        // ----------------- Encryption (for writing) ----------------

        public static (EncryptionBase Encrypter, Meta.FileCryptoMetaData CryptoMeta)
        CreateEncryptorForWrite(
            // used as the footer ENCRYPTION key in encrypted-footer mode,
            // and as the SIGNING key in plaintext-footer mode
            string encryptionOrSigningKey,
            byte[]? aadPrefixBytes,
            bool supplyAadPrefix,
            bool useCtrVariant,
            byte[]? aadFileUnique = null
        ) {
            if(string.IsNullOrWhiteSpace(encryptionOrSigningKey))
                throw new ArgumentException("Encryption/Signing key is required.", nameof(encryptionOrSigningKey));

            if(supplyAadPrefix && (aadPrefixBytes == null || aadPrefixBytes.Length == 0))
                throw new ArgumentException("SupplyAadPrefix=true requires aadPrefixBytes to be provided at write time.", nameof(aadPrefixBytes));

            byte[] key = ParseKeyString(encryptionOrSigningKey);
            if(!(key.Length is 16 or 24 or 32))
                throw new ArgumentException("AES key must be 128/192/256-bit.");

            // If the writer chose "supply prefix" mode, we must have the prefix now to compute AAD,
            // even though it will NOT be stored in the file (alg.AadPrefix = null).
            if(supplyAadPrefix && (aadPrefixBytes == null || aadPrefixBytes.Length == 0))
                throw new ArgumentException("SupplyAadPrefix=true requires aadPrefixBytes to be provided at write time.", nameof(aadPrefixBytes));

            // AAD file-unique (used in AAD suffix for all modules)
            if(aadFileUnique is null || aadFileUnique.Length == 0) {
                aadFileUnique = new byte[16];
#if NET8_0_OR_GREATER
                System.Security.Cryptography.RandomNumberGenerator.Fill(aadFileUnique);
#else
                using(var rng = System.Security.Cryptography.RandomNumberGenerator.Create())
                    rng.GetBytes(aadFileUnique);
#endif
            }

            EncryptionBase enc = useCtrVariant
                ? new AES_GCM_CTR_V1_Encryption()
                : new AES_GCM_V1_Encryption();

            // For encrypted-footer: this is the footer encryption key.
            // For plaintext-footer: this is the signing key (used only to compute/verify GCM tag).
            enc.FooterEncryptionKey = key;

            // The encrypter keeps the prefix bytes so it can compute AAD at write time,
            // regardless of whether we store it in the file.
            enc.AadFileUnique = aadFileUnique;
            enc.AadPrefix = aadPrefixBytes ?? Array.Empty<byte>();

            // Build algorithm section to serialize into FileCryptoMetaData (encrypted footer)
            // or FileMetaData.EncryptionAlgorithm (plaintext footer). If supplyAadPrefix==true,
            // do NOT store the prefix in the file.
            var alg = new Meta.EncryptionAlgorithm();
            if(useCtrVariant) {
                alg.AESGCMCTRV1 = new Meta.AesGcmCtrV1 {
                    AadFileUnique = aadFileUnique,
                    SupplyAadPrefix = supplyAadPrefix,
                    AadPrefix = supplyAadPrefix ? null : aadPrefixBytes
                };
            } else {
                alg.AESGCMV1 = new Meta.AesGcmV1 {
                    AadFileUnique = aadFileUnique,
                    SupplyAadPrefix = supplyAadPrefix,
                    AadPrefix = supplyAadPrefix ? null : aadPrefixBytes
                };
            }

            var cryptoMeta = new Meta.FileCryptoMetaData {
                EncryptionAlgorithm = alg
                // key_metadata can be filled by caller if you integrate a KMS
            };

            return (enc, cryptoMeta);
        }

        public static EncryptionBase CreateFromCryptoMeta(
            ThriftCompactProtocolReader reader,
            string footerEncryptionKey,
            string? aadPrefix
        ) {
            if(string.IsNullOrWhiteSpace(footerEncryptionKey))
                throw new ArgumentException($"Encrypted parquet files require an {nameof(footerEncryptionKey)} value");

            var cryptoMetaData = Meta.FileCryptoMetaData.Read(reader);

            EncryptionBase decrypter;
            if(cryptoMetaData.EncryptionAlgorithm.AESGCMV1 is not null) {
                decrypter = new AES_GCM_V1_Encryption();
                Meta.AesGcmV1 alg = cryptoMetaData.EncryptionAlgorithm.AESGCMV1;
                decrypter.AadFileUnique = alg.AadFileUnique
                    ?? throw new InvalidDataException("Encrypted file is missing aad_file_unique.");
                if(alg.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix))
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    decrypter.AadPrefix = Encoding.UTF8.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = alg.AadPrefix ?? Array.Empty<byte>();
                }
            } else if(cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1 is not null) {
                decrypter = new AES_GCM_CTR_V1_Encryption();
                Meta.AesGcmCtrV1 alg = cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1;
                decrypter.AadFileUnique = alg.AadFileUnique
                    ?? throw new InvalidDataException("Encrypted file is missing aad_file_unique.");
                if(alg.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix))
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    decrypter.AadPrefix = Encoding.UTF8.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = alg.AadPrefix ?? Array.Empty<byte>();
                }
            } else {
                throw new NotSupportedException("No encryption algorithm defined");
            }

            decrypter.FooterEncryptionKey = ParseKeyString(footerEncryptionKey);
            return decrypter;
        }

        /// <summary>
        /// Create a decrypter instance from a FileMetaData.EncryptionAlgorithm (plaintext footer mode).
        /// </summary>
        public static EncryptionBase CreateFromAlgorithm(
            Meta.EncryptionAlgorithm alg,
            string footerEncryptionKey,
            string? aadPrefix) {

            if(string.IsNullOrWhiteSpace(footerEncryptionKey))
                throw new ArgumentException($"Encrypted columns require a {nameof(FooterEncryptionKey)}.");

            EncryptionBase decrypter;
            if(alg.AESGCMV1 is not null) {
                Meta.AesGcmV1 a = alg.AESGCMV1;
                decrypter = new AES_GCM_V1_Encryption {
                    AadFileUnique = a.AadFileUnique ?? Array.Empty<byte>(),
                    AadPrefix = a.SupplyAadPrefix == true
                        ? (!string.IsNullOrEmpty(aadPrefix) ? Encoding.UTF8.GetBytes(aadPrefix) : throw new InvalidDataException("AAD prefix required for this file."))
                        : (a.AadPrefix ?? Array.Empty<byte>())
                };
            } else if(alg.AESGCMCTRV1 is not null) {
                Meta.AesGcmCtrV1 a = alg.AESGCMCTRV1;
                decrypter = new AES_GCM_CTR_V1_Encryption {
                    AadFileUnique = a.AadFileUnique ?? Array.Empty<byte>(),
                    AadPrefix = a.SupplyAadPrefix == true
                        ? (!string.IsNullOrEmpty(aadPrefix) ? Encoding.UTF8.GetBytes(aadPrefix) : throw new InvalidDataException("AAD prefix required for this file."))
                        : (a.AadPrefix ?? Array.Empty<byte>())
                };
            } else {
                throw new NotSupportedException("No encryption algorithm defined.");
            }

            decrypter.FooterEncryptionKey = ParseKeyString(footerEncryptionKey);
            return decrypter;
        }

        public abstract byte[] EncryptFooter(byte[] plaintext);
        public abstract byte[] EncryptColumnMetaDataWithKey(byte[] plain, short rg, short col, byte[] key);
        public abstract byte[] EncryptDataPageHeader(byte[] header, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] EncryptDataPage(byte[] body, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] EncryptDictionaryPageHeader(byte[] header, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptDictionaryPage(byte[] body, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptColumnIndex(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptOffsetIndex(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptBloomFilterHeader(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptBloomFilterBitset(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);

        internal static byte[] ParseKeyString(string keyString) {
            if(string.IsNullOrWhiteSpace(keyString))
                throw new ArgumentException(
                    "EncryptionKey must be 128/192/256-bit. " +
                    "Provide as Base64, hex, or a UTF-8 string of 16/24/32 bytes.");

            static bool IsHexChar(char c) =>
                (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F');

            bool allHexChars = keyString.All(IsHexChar);
            bool evenLength = (keyString.Length % 2) == 0;

            // 1) HEX — only if it would decode to 16/24/32 bytes
            if(evenLength && allHexChars) {
                int decodedLen = keyString.Length / 2;
                if(decodedLen is 16 or 24 or 32) {
                    byte[] bytes = new byte[decodedLen];
                    for(int i = 0; i < decodedLen; i++) {
                        int hi = Convert.ToInt32(keyString[2 * i].ToString(), 16);
                        int lo = Convert.ToInt32(keyString[(2 * i) + 1].ToString(), 16);
                        bytes[i] = (byte)((hi << 4) + lo);
                    }
                    return bytes;
                }
                // Don't throw yet; try Base64/UTF-8 fallbacks first.
            }

            // 2) Base64 (standard or URL-safe, with optional padding)
            try {
                string s = keyString.Replace('-', '+').Replace('_', '/');
                switch(s.Length % 4) {
                    case 2:
                        s += "==";
                        break;
                    case 3:
                        s += "=";
                        break;
                }
                byte[] b64 = Convert.FromBase64String(s);
                if(b64.Length is 16 or 24 or 32)
                    return b64;
            } catch {
                // ignore and fall through
            }

            // Try UTF8
            if(keyString.Length is 16 or 24 or 32) {
                try {
                    return Encoding.UTF8.GetBytes(keyString);
                } catch {
                    //ignore and fall through
                }
            }

            throw new ArgumentException(
            "EncryptionKey must be 128/192/256-bit. " +
            "Provide as Base64, hex, or a UTF-8 string of 16/24/32 bytes.");
        }

        protected static byte[] ReadExactlyOrInvalid(ThriftCompactProtocolReader reader, int length, string context) {
            try {
                return reader.ReadBytesExactly(length);
            } catch(IOException ex) {
                // Normalize framing issues to InvalidDataException for tests + callers
                throw new InvalidDataException($"{context}: expected {length} bytes but stream ended early.", ex);
            }
        }

        protected void CountGcmInvocation() {
            long v = Interlocked.Increment(ref _gcmInvocations);
            if(v > MaxInvocations) {
                throw new CryptographicException("AES-GCM invocation limit exceeded for this key.");
            }
        }

        protected void CountPageEncryption() {
            long v = Interlocked.Increment(ref _pageEncryptions);
            if(v > MaxPagesPerKey) {
                throw new CryptographicException("Per-key page encryption limit (~2^31) exceeded.");
            }
        }

        protected static byte[] FrameGcm(ReadOnlySpan<byte> nonce12, ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> tag16) {
            int payloadLen = nonce12.Length + ciphertext.Length + tag16.Length; // >= 28
            byte[] framed = new byte[4 + payloadLen];
            BinaryPrimitives.WriteInt32LittleEndian(framed.AsSpan(0, 4), payloadLen);
            nonce12.CopyTo(framed.AsSpan(4));
            ciphertext.CopyTo(framed.AsSpan(4 + nonce12.Length));
            tag16.CopyTo(framed.AsSpan(4 + nonce12.Length + ciphertext.Length));
            return framed;
        }

        // CTR pages: [lenLE][nonce(12)][ciphertext]
        protected static byte[] FrameCtr(ReadOnlySpan<byte> nonce12, ReadOnlySpan<byte> ciphertext) {
            int payloadLen = nonce12.Length + ciphertext.Length; // >= 12
            byte[] framed = new byte[4 + payloadLen];
            BinaryPrimitives.WriteInt32LittleEndian(framed.AsSpan(0, 4), payloadLen);
            nonce12.CopyTo(framed.AsSpan(4));
            ciphertext.CopyTo(framed.AsSpan(4 + nonce12.Length));
            return framed;
        }


        internal byte[] BuildAad(
                Meta.ParquetModules module,
                short? rowGroupOrdinal = null,
                short? columnOrdinal = null,
                short? pageOrdinal = null
            ) {

            byte[] prefix = AadPrefix ?? Array.Empty<byte>();
            byte[] fileUnique = AadFileUnique ?? throw new InvalidDataException("Missing AadFileUnique.");
            byte[] suffix = BuildAadSuffix(fileUnique, module, rowGroupOrdinal, columnOrdinal, pageOrdinal);

            byte[] aad = new byte[prefix.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, aad, 0, prefix.Length);
            Buffer.BlockCopy(suffix, 0, aad, prefix.Length, suffix.Length);
            return aad;
        }

        internal static byte[] BuildAadSuffix(
            byte[] aadFileUnique,
            Meta.ParquetModules module,
            short? rowGroupOrdinal,
            short? columnOrdinal,
            short? pageOrdinal
        ) {

            using var ms = new MemoryStream();
            ms.Write(aadFileUnique, 0, aadFileUnique.Length);
            ms.WriteByte((byte)module);
            if(rowGroupOrdinal.HasValue) {
                byte[] le = BitConverter.GetBytes(rowGroupOrdinal.Value).EnsureLittleEndian();
                ms.Write(le, 0, le.Length);
            }
            if(columnOrdinal.HasValue) {
                byte[] le = BitConverter.GetBytes(columnOrdinal.Value).EnsureLittleEndian();
                ms.Write(le, 0, le.Length);
            }
            if(pageOrdinal.HasValue) {
                byte[] le = BitConverter.GetBytes(pageOrdinal.Value).EnsureLittleEndian();
                ms.Write(le, 0, le.Length);
            }
            return ms.ToArray();
        }
    }
}
