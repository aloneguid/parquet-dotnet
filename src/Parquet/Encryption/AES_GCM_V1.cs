using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
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
        protected virtual byte[] Decrypt(ThriftCompactProtocolReader reader, Meta.ParquetModules module, short? rowGroupOrdinal = null, short? columnOrdinal = null, short? pageOrdinal = null) {
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
}
