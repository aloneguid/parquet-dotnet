using System;
using System.IO;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
    /// <summary>
    /// Centralizes "decrypt-or-use-plaintext" logic for modular-encrypted Parquet substreams.
    /// Only used when FileMetaData.Decrypter != null; otherwise call the existing Read(..) APIs directly.
    /// </summary>
    internal static class EncryptionReadHelpers {
        private static ThriftCompactProtocolReader ReaderFrom(byte[] bytes)
            => new ThriftCompactProtocolReader(new MemoryStream(bytes, writable: false));

        /// <summary>
        /// Page header (data). Encrypted with GCM in both AES_GCM_V1 and AES_GCM_CTR_V1.
        /// </summary>
        internal static PageHeader ReadDecryptedDataPageHeader(
            ThriftCompactProtocolReader tpr,
            Meta.FileMetaData fmd,
            short rowGroupOrdinal,
            short columnOrdinal,
            short pageOrdinal) {
            if(fmd.Decrypter == null)
                return PageHeader.Read(tpr);

            byte[] buf = fmd.Decrypter.DecryptDataPageHeader(tpr, rowGroupOrdinal, columnOrdinal, pageOrdinal);
            return PageHeader.Read(ReaderFrom(buf));
        }

        /// <summary>
        /// Page header (dictionary). Encrypted with GCM in both algorithms.
        /// </summary>
        internal static PageHeader ReadDecryptedDictionaryPageHeader(
            ThriftCompactProtocolReader tpr,
            Meta.FileMetaData fmd,
            short rowGroupOrdinal,
            short columnOrdinal,
            short pageOrdinal) {
            if(fmd.Decrypter == null)
                return PageHeader.Read(tpr);

            byte[] buf = fmd.Decrypter.DecryptDictionaryPageHeader(tpr, rowGroupOrdinal, columnOrdinal);
            return PageHeader.Read(ReaderFrom(buf));
        }

        /// <summary>
        /// Data page body. GCM in AES_GCM_V1; CTR in AES_GCM_CTR_V1 (helper uses whatever the decrypter implements).
        /// </summary>
        internal static byte[] ReadDecryptedDataPageBytes(
            ThriftCompactProtocolReader tpr,
            Meta.FileMetaData fmd,
            short rowGroupOrdinal,
            short columnOrdinal,
            short pageOrdinal) {
            if(fmd.Decrypter == null)
                throw new InvalidOperationException("ReadDecryptedDataPageBytes called without a decrypter.");

            return fmd.Decrypter.DecryptDataPage(tpr, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        }

        /// <summary>
        /// Dictionary page body. GCM in AES_GCM_V1; CTR in AES_GCM_CTR_V1.
        /// </summary>
        internal static byte[] ReadDecryptedDictionaryPageBytes(
            ThriftCompactProtocolReader tpr,
            Meta.FileMetaData fmd,
            short rowGroupOrdinal,
            short columnOrdinal) {
            if(fmd.Decrypter == null)
                throw new InvalidOperationException("ReadDecryptedDictionaryPageBytes called without a decrypter.");

            return fmd.Decrypter.DecryptDictionaryPage(tpr, rowGroupOrdinal, columnOrdinal);
        }

        internal static Meta.ColumnIndex ReadDecryptedColumnIndex(
            ThriftCompactProtocolReader tpr,
            Meta.FileMetaData fmd,
            short rowGroupOrdinal,
            short columnOrdinal) {
            if(fmd.Decrypter == null)
                return Meta.ColumnIndex.Read(tpr);
            byte[] buf = fmd.Decrypter.DecryptColumnIndex(tpr, rowGroupOrdinal, columnOrdinal);
            return Meta.ColumnIndex.Read(ReaderFrom(buf));
        }

        internal static Meta.OffsetIndex ReadDecryptedOffsetIndex(
            ThriftCompactProtocolReader tpr,
            Meta.FileMetaData fmd,
            short rowGroupOrdinal,
            short columnOrdinal) {
            if(fmd.Decrypter == null)
                return Meta.OffsetIndex.Read(tpr);
            byte[] buf = fmd.Decrypter.DecryptOffsetIndex(tpr, rowGroupOrdinal, columnOrdinal);
            return Meta.OffsetIndex.Read(ReaderFrom(buf));
        }

        // If you have Bloom filter readers, wire these similarly:
        internal static TThrift ReadDecryptedBloom<TThrift>(
            ThriftCompactProtocolReader tpr,
            Meta.FileMetaData fmd,
            short rowGroupOrdinal,
            short columnOrdinal,
            Func<ThriftCompactProtocolReader, TThrift> parse,
            bool header) {
            if(fmd.Decrypter == null)
                return parse(tpr);

            byte[] buf = header
                ? fmd.Decrypter.BloomFilterHeader(tpr, rowGroupOrdinal, columnOrdinal)
                : fmd.Decrypter.BloomFilterBitset(tpr, rowGroupOrdinal, columnOrdinal);

            return parse(ReaderFrom(buf));
        }
    }
}
