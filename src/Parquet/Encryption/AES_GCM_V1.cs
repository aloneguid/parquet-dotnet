using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Meta.Proto;
using System.Security.Cryptography;

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
            if (string.IsNullOrEmpty(decryptionKey)) {
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
                //NOTE: This hasn't been tested!!!
                //int cipherTextLength = encryptionBufferLength - nonce.Length;
                //byte[] cipherText = reader.ReadBytesExactly(cipherTextLength);

                //using var cipherStream = new MemoryStream(cipherText);
                //using var plaintextStream = new MemoryStream(); //This will contain the decrypted result
                //AesCtrTransform(keyBytes, nonce, cipherStream, plaintextStream);

                //plaintextStream.Position = 0;

                //return plaintextStream.ToArray();
                decrypter = new AES_GCM_V1_Encryption(); // AES_GCM_CTR_V1_Encryption();
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

    /// <summary>
    /// Implemented based on https://github.com/apache/parquet-format/blob/master/Encryption.md#51-encrypted-module-serialization
    /// </summary>
    internal class AES_GCM_V1_Encryption : EncryptionBase {

        public AES_GCM_V1_Encryption() {
        }

        public override byte[] BloomFilterBitset(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal) => Decrypt(reader, Meta.ParquetModules.BloomFilter_Bitset, rowGroupOrdinal, columnOrdinal);
        public override byte[] BloomFilterHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal) => Decrypt(reader, Meta.ParquetModules.BloomFilter_Header, rowGroupOrdinal, columnOrdinal);
        public override byte[] DecryptColumnIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal) => Decrypt(reader, Meta.ParquetModules.ColumnIndex, rowGroupOrdinal, columnOrdinal);
        public override byte[] DecryptColumnMetaData(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal) => Decrypt(reader, Meta.ParquetModules.ColumnMetaData, rowGroupOrdinal, columnOrdinal);
        public override byte[] DecryptDataPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal) => Decrypt(reader, Meta.ParquetModules.Data_Page, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        public override byte[] DecryptDataPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal) => Decrypt(reader, Meta.ParquetModules.Data_PageHeader, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        public override byte[] DecryptDictionaryPage(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal) => Decrypt(reader, Meta.ParquetModules.Dictionary_Page, rowGroupOrdinal, columnOrdinal);
        public override byte[] DecryptDictionaryPageHeader(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal) => Decrypt(reader, Meta.ParquetModules.Dictionary_PageHeader, rowGroupOrdinal, columnOrdinal);
        protected override byte[] DecryptFooter(ThriftCompactProtocolReader reader) => Decrypt(reader, Meta.ParquetModules.Footer);
        public override byte[] DecryptOffsetIndex(ThriftCompactProtocolReader reader, short rowGroupOrdinal, short columnOrdinal) => Decrypt(reader, Meta.ParquetModules.OffsetIndex, rowGroupOrdinal, columnOrdinal);

        /// <summary>
        /// Module format: length (4 bytes) nonce (12 bytes) ciphertext (length-28 bytes) tag (16 bytes)
        /// Reference: https://github.com/apache/parquet-format/blob/master/Encryption.md#5-file-format
        /// </summary>
        private byte[] Decrypt(ThriftCompactProtocolReader reader, Meta.ParquetModules module, short? rowGroupOrdinal = null, short? columnOrdinal = null, short? pageOrdinal = null) {
            IEnumerable<byte> aadSuffix = AadFileUnique!
                .Concat(new byte[] { (byte)module })
                .Concat(rowGroupOrdinal != null ? BitConverter.GetBytes((short)rowGroupOrdinal).EnsureLittleEndian() : Array.Empty<byte>())
                .Concat(columnOrdinal != null ? BitConverter.GetBytes((short)columnOrdinal).EnsureLittleEndian() : Array.Empty<byte>())
                .Concat(pageOrdinal != null ? BitConverter.GetBytes((short)pageOrdinal).EnsureLittleEndian() : Array.Empty<byte>());

            byte[] tag = new byte[16];
            byte[] encryptionBufferLengthBytes = reader.ReadBytesExactly(4).EnsureLittleEndian();
            int encryptionBufferLength = BitConverter.ToInt32(encryptionBufferLengthBytes, 0);
            byte[] nonce = reader.ReadBytesExactly(12);
            int cipherTextLength = encryptionBufferLength - nonce.Length - tag.Length;
            byte[] cipherText = reader.ReadBytesExactly(cipherTextLength);
            tag = reader.ReadBytesExactly(tag.Length);

#if NETSTANDARD2_0
            throw new NotSupportedException("Cannot process AES GCM V1 encrypted parquet files in .net standard 2.0. Maybe try AES GCM CTR V1 instead?");
#elif NET8_0_OR_GREATER
            using var cipher = new AesGcm(DecryptionKey!, tag.Length);
            byte[] plainText = new byte[cipherTextLength];
            cipher.Decrypt(nonce, cipherText, tag, plainText, AadPrefix!.Concat(aadSuffix).ToArray());
            return plainText;
#else
            using var cipher = new AesGcm(DecryptionKey!);
            byte[] plainText = new byte[cipherTextLength];
            cipher.Decrypt(nonce, cipherText, tag, plainText, AadPrefix!.Concat(aadSuffix).ToArray());
            return plainText;
#endif
        }
    }


    //shorternal class AES_GCM_CTR_V1_Encryption : EncryptionBase {
    //    public AES_GCM_CTR_V1_Encryption() {

    //    }

    //    public override byte[] Decrypt(ThriftCompactProtocolReader reader, Meta.ParquetModules module) => throw new NotImplementedException();
    //}
}
