using System;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
    internal abstract class EncryptionBase {
        internal byte[]? AadPrefix { get; set; }
        internal byte[]? DecryptionKey { get; set; }
        internal byte[]? AadFileUnique { get; set; }

        public static byte[] DecryptFooter(
            ThriftCompactProtocolReader reader,
            string decryptionKey,
            string? aadPrefix,
            out EncryptionBase decrypter) {
            if(string.IsNullOrEmpty(decryptionKey)) {
                throw new ArgumentException($"Encrypted parquet files require an {nameof(ParquetOptions.EncryptionKey)} value");
            }

            var cryptoMetaData = Meta.FileCryptoMetaData.Read(reader);
            if(cryptoMetaData.EncryptionAlgorithm.AESGCMV1 is not null) {
                decrypter = new AES_GCM_V1_Encryption();
                decrypter.AadFileUnique = cryptoMetaData.EncryptionAlgorithm.AESGCMV1.AadFileUnique ?? Array.Empty<byte>();
                if(cryptoMetaData.EncryptionAlgorithm.AESGCMV1.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix)) {
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    }
                    decrypter.AadPrefix = Encoding.ASCII.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = cryptoMetaData.EncryptionAlgorithm.AESGCMV1.AadPrefix ?? Array.Empty<byte>();
                }
            } else if(cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1 is not null) {
                decrypter = new AES_GCM_CTR_V1_Encryption();

                Meta.AesGcmCtrV1 alg = cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1;
                decrypter.AadFileUnique = alg.AadFileUnique ?? Array.Empty<byte>();

                if(alg.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix))
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    decrypter.AadPrefix = Encoding.ASCII.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = alg.AadPrefix ?? Array.Empty<byte>();
                }
            } else {
                throw new NotSupportedException("No encryption algorithm defined");
            }

            decrypter.DecryptionKey = ParseKeyString(decryptionKey);
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
        CreateEncrypterForWrite(
            string encryptionKey,
            byte[]? aadPrefixBytes,
            bool supplyAadPrefix,
            bool useCtrVariant,
            byte[]? aadFileUnique = null
        ) {
            if(string.IsNullOrWhiteSpace(encryptionKey))
                throw new ArgumentException("Encryption key is required.", nameof(encryptionKey));

            byte[] key = ParseKeyString(encryptionKey);
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

            enc.DecryptionKey = key;
            enc.AadFileUnique = aadFileUnique;
            enc.AadPrefix = aadPrefixBytes ?? Array.Empty<byte>();

            // Build algorithm section
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
            };

            return (enc, cryptoMeta);
        }

        public static EncryptionBase CreateFromCryptoMeta(
            ThriftCompactProtocolReader reader,
            string decryptionKey,
            string? aadPrefix
        ) {
            if(string.IsNullOrWhiteSpace(decryptionKey))
                throw new ArgumentException($"Encrypted parquet files require an {nameof(ParquetOptions.EncryptionKey)} value");

            var cryptoMetaData = Meta.FileCryptoMetaData.Read(reader);

            EncryptionBase decrypter;
            if(cryptoMetaData.EncryptionAlgorithm.AESGCMV1 is not null) {
                decrypter = new AES_GCM_V1_Encryption();
                Meta.AesGcmV1 alg = cryptoMetaData.EncryptionAlgorithm.AESGCMV1;
                decrypter.AadFileUnique = alg.AadFileUnique ?? Array.Empty<byte>();
                if(alg.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix))
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    decrypter.AadPrefix = Encoding.ASCII.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = alg.AadPrefix ?? Array.Empty<byte>();
                }
            } else if(cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1 is not null) {
                decrypter = new AES_GCM_CTR_V1_Encryption();
                Meta.AesGcmCtrV1 alg = cryptoMetaData.EncryptionAlgorithm.AESGCMCTRV1;
                decrypter.AadFileUnique = alg.AadFileUnique ?? Array.Empty<byte>();
                if(alg.SupplyAadPrefix == true) {
                    if(string.IsNullOrEmpty(aadPrefix))
                        throw new InvalidDataException("This file requires an AAD (additional authenticated data) prefix in order to be decrypted.");
                    decrypter.AadPrefix = Encoding.ASCII.GetBytes(aadPrefix);
                } else {
                    decrypter.AadPrefix = alg.AadPrefix ?? Array.Empty<byte>();
                }
            } else {
                throw new NotSupportedException("No encryption algorithm defined");
            }

            decrypter.DecryptionKey = ParseKeyString(decryptionKey);
            return decrypter;
        }


        public abstract byte[] EncryptFooter(byte[] plaintext);
        public abstract byte[] EncryptColumnMetaData(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptDataPageHeader(byte[] header, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] EncryptDataPage(byte[] body, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] EncryptDictionaryPageHeader(byte[] header, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptDictionaryPage(byte[] body, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptColumnIndex(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptOffsetIndex(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptBloomFilterHeader(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] EncryptBloomFilterBitset(byte[] bytes, short rowGroupOrdinal, short columnOrdinal);

        private static byte[] ParseKeyString(string keyString) {
            // 1) Try Base64
            try {
                // Reject whitespace-only or obviously invalid strings fast
                if(!string.IsNullOrWhiteSpace(keyString)) {
                    byte[] b64 = Convert.FromBase64String(keyString);
                    if(b64.Length is 16 or 24 or 32)
                        return b64;
                }
            } catch { /* ignore */ }

            // 2) Try hex (even length, 0-9a-fA-F)
            bool looksHex = keyString.Length % 2 == 0 && keyString.All(c =>
                (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F'));
            if(looksHex) {
                int len = keyString.Length / 2;
                byte[] bytes = new byte[len];
                for(int i = 0; i < len; i++) {
                    int hi = Convert.ToInt32(keyString[2 * i].ToString(), 16);
                    int lo = Convert.ToInt32(keyString[(2 * i) + 1].ToString(), 16);
                    bytes[i] = (byte)((hi << 4) + lo);
                }
                if(bytes.Length is 16 or 24 or 32)
                    return bytes;
            }

            // 3) Fallback: raw UTF-8
            byte[] utf8 = System.Text.Encoding.UTF8.GetBytes(keyString);
            if(utf8.Length is 16 or 24 or 32)
                return utf8;

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
    }
}
