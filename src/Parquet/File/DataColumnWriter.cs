using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using IronCompress;
using Microsoft.IO;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.File {
    class DataColumnWriter {
        private readonly Stream _stream;
        private readonly ThriftFooter _footer;
        private readonly SchemaElement _schemaElement;
        private readonly CompressionMethod _compressionMethod;
        private readonly CompressionLevel _compressionLevel;
        private readonly Dictionary<string, string>? _keyValueMetadata;
        private readonly ParquetOptions _options;
        private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();
        private readonly short _rowGroupOrdinal;
        private readonly short _columnOrdinal;
        private short _pageOrdinal; // increments per DATA page only

        public DataColumnWriter(
            Stream stream,
            ThriftFooter footer,
            SchemaElement schemaElement,
            CompressionMethod compressionMethod,
            ParquetOptions options,
            CompressionLevel compressionLevel,
            Dictionary<string, string>? keyValueMetadata,
            short rowGroupOrdinal,
            short columnOrdinal
        ) {
            _stream = stream;
            _footer = footer;
            _schemaElement = schemaElement;
            _compressionMethod = compressionMethod;
            _compressionLevel = compressionLevel;
            _keyValueMetadata = keyValueMetadata;
            _options = options;
            _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
            _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;
            _rowGroupOrdinal = rowGroupOrdinal;
            _columnOrdinal = columnOrdinal;
            _pageOrdinal = 0;
        }

        public async Task<ColumnChunk> WriteAsync(
            FieldPath fullPath,
            DataColumn column,
            CancellationToken cancellationToken = default
        ) {

            if(column == null)
                throw new ArgumentNullException(nameof(column));
            column.Field.EnsureAttachedToSchema(nameof(column));

            // Create the chunk as before
            ColumnChunk chunk = _footer.CreateColumnChunk(
                _compressionMethod, _stream, _schemaElement.Type!.Value, fullPath, column.NumValues, _keyValueMetadata);

            // Will we encrypt this column at all?
            bool writerHasEncrypter = _footer.Encrypter is not null;

            // Decide if this column uses a column-specific key or the footer key
            bool useColumnKey = false;
            byte[]? columnKeyBytes = null;
            byte[]? columnKeyMetadata = null;

            if(writerHasEncrypter) {
                // Build a stable path string to match user-supplied map (you can adapt to your convention)
                string pathStr = string.Join(".", fullPath.ToList());

                if(_options is not null &&
                   _options.ColumnKeys is not null &&
                   _options.ColumnKeys.TryGetValue(pathStr, out ParquetOptions.ColumnKeySpec? spec)) {

                    useColumnKey = true;
                    columnKeyBytes = Encryption.EncryptionBase.ParseKeyString(spec.Key);
                    columnKeyMetadata = spec.KeyMetadata;

                    // Advertise column crypto metadata (column-key case)
                    chunk.CryptoMetadata = new ColumnCryptoMetaData {
                        ENCRYPTIONWITHCOLUMNKEY = new EncryptionWithColumnKey {
                            PathInSchema = fullPath.ToList(),
                            KeyMetadata = columnKeyMetadata
                        }
                    };
                } else {
                    // Footer-key case
                    chunk.CryptoMetadata = new ColumnCryptoMetaData {
                        ENCRYPTIONWITHFOOTERKEY = new EncryptionWithFooterKey()
                    };
                }
            }

            // If we’re using a column key, temporarily swap it onto the encrypter so that
            // *all* per-column modules (page headers/bodies, indexes, bloom) are sealed with that key.
            byte[]? originalKey = null;
            if(writerHasEncrypter && useColumnKey) {
                originalKey = _footer.Encrypter!.FooterEncryptionKey;
                _footer.Encrypter.FooterEncryptionKey = columnKeyBytes!;
            }

            ColumnSizes sizes;
            try {
                // This writes dictionary + data pages (and encrypts them if encrypter != null)
                sizes = await WriteColumnAsync(chunk, column, _schemaElement, cancellationToken);
            } finally {
                if(writerHasEncrypter && useColumnKey) {
                    _footer.Encrypter!.FooterEncryptionKey = originalKey!;
                }
            }

            // Generate/attach stats to the (plaintext) ColumnMetaData we just produced
            chunk.MetaData!.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);

            // Counters include page headers + bodies
            chunk.MetaData.TotalCompressedSize = sizes.CompressedSize;
            chunk.MetaData.TotalUncompressedSize = sizes.UncompressedSize;

            // ---- ColumnMetaData protection per spec (§5.3/§5.4) ----
            // If the column uses a *column-specific* key, we must serialize ColumnMetaData separately
            // and encrypt it with the column key, storing the result in encrypted_column_metadata.
            if(writerHasEncrypter && useColumnKey) {
                // 1) Serialize ColumnMetaData
                byte[] cmdPlain;
                using(var ms = new MemoryStream()) {
                    chunk.MetaData!.Write(new Meta.Proto.ThriftCompactProtocolWriter(ms));
                    cmdPlain = ms.ToArray();
                }

                // 2) Encrypt with the *column key* (swap again just for this operation)
                byte[]? originalKey2 = _footer.Encrypter!.FooterEncryptionKey;
                _footer.Encrypter.FooterEncryptionKey = columnKeyBytes!;
                try {
                    byte[] encCmd = _footer.Encrypter.EncryptColumnMetaData(
                        cmdPlain, _rowGroupOrdinal, _columnOrdinal);
                    chunk.EncryptedColumnMetadata = encCmd;
                    chunk.MetaData = null; // clear plaintext copy
                } finally {
                    _footer.Encrypter.FooterEncryptionKey = originalKey2!;
                }

                // 3) Adjust where meta_data lives according to footer mode:
                //    - Encrypted footer mode: omit meta_data entirely.
                //    - Plaintext footer mode: keep meta_data but strip sensitive stats so legacy readers can vectorize.
                bool encryptedFooterMode = _footer.Encrypter is not null && !(_options?.UsePlaintextFooter ?? false);
                if(encryptedFooterMode) {
                    // chunk.MetaData = null;
                } else {
                    if(chunk.MetaData?.Statistics != null) {
                        chunk.MetaData.Statistics.MinValue = null;
                        chunk.MetaData.Statistics.MaxValue = null;
                        chunk.MetaData.Statistics.NullCount = null;
                        chunk.MetaData.Statistics.DistinctCount = null;
                    }
                }
            }

            return chunk;
        }

        class ColumnSizes {
            public int CompressedSize;
            public int UncompressedSize;
        }

        private async Task CompressAndWriteAsync(
            PageHeader ph,
            MemoryStream data,
            ColumnSizes cs,
            CancellationToken cancellationToken
        ) {
            // Compress (or pass-through) the page body first
            byte[]? borrowed;
            ReadOnlySpan<byte> plain = GetBytesFast(data, out borrowed);
            using IronCompress.IronCompressResult compressedData =
                _compressionMethod == CompressionMethod.None
                    ? new IronCompress.IronCompressResult(plain.ToArray(), Codec.Snappy, false)
                    : Compressor.Compress(_compressionMethod, plain, _compressionLevel);

            // Plaintext path (no encryption)
            if(_footer.Encrypter is null) {
                ph.UncompressedPageSize = (int)data.Length;
                ph.CompressedPageSize = compressedData.AsSpan().Length;

                using MemoryStream headerMs = _rmsMgr.GetStream();
                ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
                int headerSize = (int)headerMs.Length;
                headerMs.Position = 0;

                await headerMs.CopyToAsync(_stream, 81920, cancellationToken: cancellationToken);
                _stream.WriteSpan(compressedData);

                cs.CompressedSize += headerSize + ph.CompressedPageSize;
                cs.UncompressedSize += headerSize + ph.UncompressedPageSize;
                return;
            }

            // Encrypted path
            // 1) Set plaintext uncompressed size in header
            ph.UncompressedPageSize = (int)data.Length;

            // 2) Prepare plaintext body bytes to encrypt
            byte[] bodyBytes = compressedData.AsSpan().ToArray();

            // 3) Encrypt page BODY first so we know the encrypted length
            //    GCM profile: page bodies must be framed as [nonce(12)][ciphertext][tag(16)]
            //    (No 4-byte length prefix in the page body frame)
            byte[] encBody = ph.Type == PageType.DICTIONARY_PAGE
                ? _footer.Encrypter.EncryptDictionaryPage(bodyBytes, _rowGroupOrdinal, _columnOrdinal)
                : _footer.Encrypter.EncryptDataPage(bodyBytes, _rowGroupOrdinal, _columnOrdinal, _pageOrdinal);

            // 4) Now set the header's CompressedPageSize to the ENCRYPTED body length
            // https://github.com/apache/parquet-format/blob/master/Encryption.md#51-encrypted-module-serialization
            ph.CompressedPageSize = encBody.Length;

            // 5) Serialize the PLAINTEXT header (with corrected sizes)
            byte[] headerBytes;
            using(MemoryStream headerMs = _rmsMgr.GetStream()) {
                ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
                headerBytes = headerMs.ToArray();
            }

            // 6) Encrypt the header (header framing remains as per your GCM framing helper)
            byte[] encHeader = ph.Type == PageType.DICTIONARY_PAGE
                ? _footer.Encrypter.EncryptDictionaryPageHeader(headerBytes, _rowGroupOrdinal, _columnOrdinal)
                : _footer.Encrypter.EncryptDataPageHeader(headerBytes, _rowGroupOrdinal, _columnOrdinal, _pageOrdinal);

            // 7) Write encrypted header + encrypted body
            await _stream.WriteAsync(encHeader, 0, encHeader.Length, cancellationToken);
            await _stream.WriteAsync(encBody, 0, encBody.Length, cancellationToken);

            // 8) Update counters: "compressed" = on-disk encrypted sizes; "uncompressed" = original plain sizes
            cs.CompressedSize += encHeader.Length + encBody.Length;
            cs.UncompressedSize += headerBytes.Length + ph.UncompressedPageSize;
        }

        private async Task<ColumnSizes> WriteColumnAsync(ColumnChunk chunk, DataColumn column,
           SchemaElement tse,
           CancellationToken cancellationToken = default) {

            column.Field.EnsureAttachedToSchema(nameof(column));

            var r = new ColumnSizes();

            /*
             * Page header must preceeed actual data (compressed or not) however it contains both
             * the uncompressed and compressed data size which we don't know! This somehow limits
             * the write efficiency.
             */

            using var pc = new PackedColumn(column);
            pc.Pack(_options.UseDictionaryEncoding, _options.DictionaryEncodingThreshold);

            // dictionary page
            if(pc.HasDictionary) {
                chunk.MetaData!.DictionaryPageOffset = _stream.Position;
                PageHeader ph = _footer.CreateDictionaryPage(pc.Dictionary!.Length);
                using MemoryStream ms = _rmsMgr.GetStream();
                ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                       tse,
                       ms, column.Statistics);
                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
            }

            // data page
            using(MemoryStream ms = _rmsMgr.GetStream()) {
                chunk.MetaData!.DataPageOffset = _stream.Position;
                bool deltaEncode = column.IsDeltaEncodable && _options.UseDeltaBinaryPackedEncoding;
                // data page Num_values also does include NULLs
                PageHeader ph = _footer.CreateDataPage(column.NumValues, pc.HasDictionary, deltaEncode);
                if(pc.HasRepetitionLevels) {
                    WriteLevels(ms, pc.RepetitionLevels!, pc.RepetitionLevels!.Length, column.Field.MaxRepetitionLevel);
                }
                if(pc.HasDefinitionLevels) {
                    WriteLevels(ms, pc.DefinitionLevels!, column.DefinitionLevels!.Length, column.Field.MaxDefinitionLevel);
                }

                if(pc.HasDictionary) {
                    // dictionary indexes are always encoded with RLE
                    int[] indexes = pc.GetDictionaryIndexes(out int indexesLength)!;
                    int bitWidth = pc.Dictionary!.Length.GetBitWidth();
                    ms.WriteByte((byte)bitWidth);   // bit width is stored as 1 byte before encoded data
                    RleBitpackedHybridEncoder.Encode(ms, indexes.AsSpan(0, indexesLength), bitWidth);
                } else {
                    Array data = pc.GetPlainData(out int offset, out int count);
                    if(deltaEncode) {
                        DeltaBinaryPackedEncoder.Encode(data, offset, count, ms, column.Statistics);
                        chunk.MetaData!.Encodings[2] = Encoding.DELTA_BINARY_PACKED;
                    } else {
                        ParquetPlainEncoder.Encode(data, offset, count, tse, ms, pc.HasDictionary ? null : column.Statistics);
                    }
                }

                ph.DataPageHeader!.Statistics = column.Statistics.ToThriftStatistics(tse);
                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
                _pageOrdinal++;
            }

            return r;
        }

        private static void WriteLevels(Stream s, Span<int> levels, int count, int maxValue) {
            int bitWidth = maxValue.GetBitWidth();
            RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.Slice(0, count));
        }

        private static ReadOnlySpan<byte> GetBytesFast(MemoryStream ms, out byte[]? borrowedArray) {
            borrowedArray = null;
            ArraySegment<byte> seg;
            if(ms.TryGetBuffer(out seg)) {
                return new ReadOnlySpan<byte>(seg.Array, seg.Offset, seg.Count);
            }
            byte[] arr = ms.ToArray();
            borrowedArray = arr;
            return new ReadOnlySpan<byte>(arr);
        }
    }
}
