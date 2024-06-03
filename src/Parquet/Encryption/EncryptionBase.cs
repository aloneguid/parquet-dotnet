using System;
using System.IO;
using System.Text;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
    internal abstract class EncryptionBase {
        protected byte[]? AadPrefix { get; set; }
        protected byte[]? DecryptionKey { get; set; }
        protected byte[]? AadFileUnique { get; set; }

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
            } else {
                throw new NotSupportedException("No encryption algorithm defined");
            }

            decrypter.DecryptionKey = Encoding.ASCII.GetBytes(decryptionKey);
            return decrypter.DecryptFooter(reader);
        }

        protected abstract byte[] DecryptFooter(ThriftCompactProtocolReader reader);
        public abstract byte[] DecryptColumnMetaData(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptDataPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] DecryptDictionaryPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptDataPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal);
        public abstract byte[] DecryptDictionaryPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptColumnIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] DecryptOffsetIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] BloomFilterHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
        public abstract byte[] BloomFilterBitset(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal);
    }
}
