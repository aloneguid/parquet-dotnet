using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using IronCompress;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Encodings;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Extensions;
using System.Collections.Generic;

namespace Parquet.File {

    /// <summary>
    /// Reader for Parquet data column
    /// </summary>
    class DataColumnReader {
        private readonly DataField _dataField;
        private readonly Stream _inputStream;
        private readonly ColumnChunk _thriftColumnChunk;
        private readonly SchemaElement? _schemaElement;
        private readonly ThriftFooter _footer;
        private readonly ParquetOptions _options;
        private readonly DataColumnStatistics? _stats;
        private readonly RowGroup _rowGroup;

        internal DataColumnReader(
           DataField dataField,
           Stream inputStream,
           ColumnChunk thriftColumnChunk,
           DataColumnStatistics? stats,
           ThriftFooter footer,
           ParquetOptions? parquetOptions,
           RowGroup rowGroup) {
            _dataField = dataField ?? throw new ArgumentNullException(nameof(dataField));
            _inputStream = inputStream ?? throw new ArgumentNullException(nameof(inputStream));
            _thriftColumnChunk = thriftColumnChunk ?? throw new ArgumentNullException(nameof(thriftColumnChunk));
            _stats = stats;
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _options = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));
            _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));

            dataField.EnsureAttachedToSchema(nameof(dataField));

            _schemaElement = _footer.GetSchemaElement(_thriftColumnChunk);
        }

        /// <summary>
        /// Return data column statistics
        /// </summary>
        /// <returns>Data column statistics or null</returns>
        public DataColumnStatistics? GetColumnStatistics() => _stats;

        /// <summary>
        /// Read entire column data
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>DataColumn object filled in with data</returns>
        /// <exception cref="NotSupportedException">Unsupported page type</exception>
        public async Task<DataColumn> ReadAsync(CancellationToken cancellationToken = default) {

            // how many values are in column chunk, as there may be multiple data pages
            int totalValuesInChunk = (int)_thriftColumnChunk.MetaData!.NumValues;
            int definedValuesCount = totalValuesInChunk;
            if(_stats?.NullCount != null)
                definedValuesCount -= (int)_stats.NullCount.Value;

            using var pc = new PackedColumn(_dataField, totalValuesInChunk, definedValuesCount);

            // seek to first page (dict or data)
            long fileOffset = GetFileOffset(out bool isDictionaryPageOffset);
            _inputStream.Seek(fileOffset, SeekOrigin.Begin);

            // NEW: only treat this column as encrypted when the chunk *has* CryptoMetadata
            bool useEncryption = _thriftColumnChunk.CryptoMetadata is not null;

            short pageOrdinal = 0;
            short columnOrdinal = (short)_rowGroup.Columns.IndexOf(_thriftColumnChunk);
            if(columnOrdinal < 0)
                throw new InvalidDataException("Could not determine column ordinal");

            while(pc.ValuesRead < totalValuesInChunk) {
                if(useEncryption) {

                    // Dictionary (encrypted) if positioned there
                    if(isDictionaryPageOffset) {
                        byte[] dictHdr = _footer.Decrypter!.DecryptDictionaryPageHeader(
                            new ThriftCompactProtocolReader(_inputStream),
                            _rowGroup.Ordinal!.Value, columnOrdinal);

                        using(var ms = new MemoryStream(dictHdr)) {
                            var hdrReader = new ThriftCompactProtocolReader(ms);
                            var ph = PageHeader.Read(hdrReader);

                            byte[] dictBody = _footer.Decrypter.DecryptDictionaryPage(
                                new ThriftCompactProtocolReader(_inputStream),
                                _rowGroup.Ordinal!.Value, columnOrdinal);

                            ReadDictionaryPageFromBuffer(ph, dictBody, pc);
                        }

                        isDictionaryPageOffset = false;
                        continue;
                    }

                    // Footer-key encrypted column
                    if(_thriftColumnChunk.CryptoMetadata?.ENCRYPTIONWITHFOOTERKEY != null) {
                        if(_footer.Decrypter is null)
                            throw new InvalidDataException(
                                "This file contains encrypted columns, but no footer key/decrypter is available. " +
                                "Provide ParquetOptions.FooterEncryptionKey (and AAD prefix if required).");
                        // header (GCM)
                        byte[] hdr = _footer.Decrypter!.DecryptDataPageHeader(
                            new ThriftCompactProtocolReader(_inputStream),
                            _rowGroup.Ordinal!.Value, columnOrdinal, pageOrdinal);

                        using var ms = new MemoryStream(hdr);
                        var hdrReader = new ThriftCompactProtocolReader(ms);
                        var ph = PageHeader.Read(hdrReader);

                        // body (algo depends on profile; decrypter handles it)
                        byte[] body = _footer.Decrypter.DecryptDataPage(
                            new ThriftCompactProtocolReader(_inputStream),
                            _rowGroup.Ordinal!.Value, columnOrdinal, pageOrdinal);

                        if(ph.Type == PageType.DATA_PAGE)
                            ReadDataPageV1FromBuffer(ph, body, pc);
                        else if(ph.Type == PageType.DATA_PAGE_V2)
                            ReadDataPageV2FromBuffer(ph, body, pc, totalValuesInChunk);
                        else if(ph.Type == PageType.DICTIONARY_PAGE)
                            ReadDictionaryPageFromBuffer(ph, body, pc);
                        else
                            throw new InvalidDataException($"Unsupported page type '{ph.Type}'");

                        pageOrdinal++;
                    } else if(_thriftColumnChunk.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY != null) {
                        EncryptionWithColumnKey ck = _thriftColumnChunk.CryptoMetadata.ENCRYPTIONWITHCOLUMNKEY;
                        string? keyString = _options.ColumnKeyResolver?.Invoke(ck.PathInSchema, ck.KeyMetadata);

                        if(string.IsNullOrWhiteSpace(keyString))
                            throw new InvalidDataException(
                                $"Column key is required to read encrypted column '{string.Join(".", ck.PathInSchema)}'.");

                        byte[] columnKey = Encryption.EncryptionBase.ParseKeyString(keyString!);

                        // Temporarily swap the decrypter key for this column
                        byte[]? originalKey = _footer.Decrypter!.FooterEncryptionKey;
                        _footer.Decrypter.FooterEncryptionKey = columnKey;
                        try {
                            // Dictionary page header/body (GCM)
                            if(isDictionaryPageOffset) {
                                byte[] dictHdr = _footer.Decrypter!.DecryptDictionaryPageHeader(
                                    new ThriftCompactProtocolReader(_inputStream),
                                    _rowGroup.Ordinal!.Value, columnOrdinal);

                                using var ms = new MemoryStream(dictHdr);
                                var hdrReader2 = new ThriftCompactProtocolReader(ms);
                                var ph = PageHeader.Read(hdrReader2);

                                byte[] dictBody = _footer.Decrypter.DecryptDictionaryPage(
                                    new ThriftCompactProtocolReader(_inputStream),
                                    _rowGroup.Ordinal!.Value, columnOrdinal);

                                ReadDictionaryPageFromBuffer(ph, dictBody, pc);
                                isDictionaryPageOffset = false;
                            }

                            // Data pages loop (same as footer-key path, but using the swapped key)
                            // header (GCM)
                            byte[] hdr = _footer.Decrypter!.DecryptDataPageHeader(
                                new ThriftCompactProtocolReader(_inputStream),
                                _rowGroup.Ordinal!.Value, columnOrdinal, pageOrdinal);

                            using var msHdr = new MemoryStream(hdr);
                            var hdrReader = new ThriftCompactProtocolReader(msHdr);
                            var ph2 = PageHeader.Read(hdrReader);

                            // body
                            byte[] body = _footer.Decrypter.DecryptDataPage(
                                new ThriftCompactProtocolReader(_inputStream),
                                _rowGroup.Ordinal!.Value, columnOrdinal, pageOrdinal);

                            if(ph2.Type == PageType.DATA_PAGE)
                                ReadDataPageV1FromBuffer(ph2, body, pc);
                            else if(ph2.Type == PageType.DATA_PAGE_V2)
                                ReadDataPageV2FromBuffer(ph2, body, pc, totalValuesInChunk);
                            else if(ph2.Type == PageType.DICTIONARY_PAGE)
                                ReadDictionaryPageFromBuffer(ph2, body, pc);
                            else
                                throw new InvalidDataException($"Unsupported page type '{ph2.Type}'");

                            pageOrdinal++;
                        } finally {
                            _footer.Decrypter!.FooterEncryptionKey = originalKey!;
                        }
                    } else {
                        // Shouldn't happen because useEncryption implies CryptoMetadata != null,
                        // but keep a defensive fallback to plaintext.
                        var plainReader = new ThriftCompactProtocolReader(_inputStream);
                        PageHeader ph = PageHeader.Read(plainReader);

                        switch(ph.Type) {
                            case PageType.DICTIONARY_PAGE:
                                await ReadDictionaryPage(ph, pc);
                                break;
                            case PageType.DATA_PAGE:
                                await ReadDataPageV1Async(ph, pc);
                                break;
                            case PageType.DATA_PAGE_V2:
                                await ReadDataPageV2Async(ph, pc, totalValuesInChunk);
                                break;
                            default:
                                throw new NotSupportedException($"can't read page type {ph.Type}");
                        }
                    }
                } else {
                    // Plaintext column (either plaintext file, or encrypted file with plaintext columns)
                    var protoReader = new ThriftCompactProtocolReader(_inputStream);
                    PageHeader ph = PageHeader.Read(protoReader);

                    switch(ph.Type) {
                        case PageType.DICTIONARY_PAGE:
                            await ReadDictionaryPage(ph, pc);
                            break;
                        case PageType.DATA_PAGE:
                            await ReadDataPageV1Async(ph, pc);
                            break;
                        case PageType.DATA_PAGE_V2:
                            await ReadDataPageV2Async(ph, pc, totalValuesInChunk);
                            break;
                        default:
                            throw new NotSupportedException($"can't read page type {ph.Type}");
                    }
                }
            }

            // all data ready
            DataColumn column = pc.Unpack();
            if(_stats != null)
                column.Statistics = _stats;
            return column;
        }

        private static Span<byte> AsMutableSpan(ReadOnlySpan<byte> src, out byte[] rented) {
            rented = ArrayPool<byte>.Shared.Rent(src.Length);
            src.CopyTo(rented);
            return rented.AsSpan(0, src.Length);
        }

        private async Task<IronCompress.IronCompressResult> ReadPageDataAsync(PageHeader ph) {

            byte[] data = ArrayPool<byte>.Shared.Rent(ph.CompressedPageSize);

            int totalBytesRead = 0, remainingBytes = ph.CompressedPageSize;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            if(_thriftColumnChunk.MetaData!.Codec == CompressionCodec.UNCOMPRESSED) {
                return new IronCompress.IronCompressResult(data, Codec.Snappy, false, ph.CompressedPageSize, ArrayPool<byte>.Shared);
            }

            return Compressor.Decompress((CompressionMethod)(int)_thriftColumnChunk.MetaData.Codec,
                data.AsSpan(0, ph.CompressedPageSize),
                ph.UncompressedPageSize);
        }

        private async Task<IronCompress.IronCompressResult> ReadPageDataV2Async(PageHeader ph) {

            int pageSize = ph.CompressedPageSize;

            byte[] data = ArrayPool<byte>.Shared.Rent(pageSize);

            int totalBytesRead = 0, remainingBytes = pageSize;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            return new IronCompress.IronCompressResult(data, Codec.Snappy, false, pageSize, ArrayPool<byte>.Shared);
        }

        private async ValueTask ReadDictionaryPage(PageHeader ph, PackedColumn pc) {

            if(pc.HasDictionary)
                throw new InvalidOperationException("dictionary already read");

            //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.
            using IronCompress.IronCompressResult bytes = await ReadPageDataAsync(ph);

            // Dictionary should not contains null values
            Array dictionary = _dataField.CreateArray(ph.DictionaryPageHeader!.NumValues);

            ParquetPlainEncoder.Decode(dictionary, 0, ph.DictionaryPageHeader.NumValues,
                   _schemaElement!, bytes.AsSpan(), out int dictionaryOffset);

            pc.AssignDictionary(dictionary);
        }

        private long GetFileOffset(out bool isDictionaryPageOffset) {
            //https://stackoverflow.com/a/55226688/1458738
            long dictionaryPageOffset = _thriftColumnChunk.MetaData?.DictionaryPageOffset ?? 0;
            long firstDataPageOffset = _thriftColumnChunk.MetaData!.DataPageOffset;
            if(dictionaryPageOffset > 0 && dictionaryPageOffset < firstDataPageOffset) {
                // if there's a dictionary and it's before the first data page, start from there
                isDictionaryPageOffset = true;
                return dictionaryPageOffset;
            }
            isDictionaryPageOffset = false;
            return firstDataPageOffset;
        }

        private async Task ReadDataPageV1Async(PageHeader ph, PackedColumn pc) {
            using IronCompress.IronCompressResult bytes = await ReadPageDataAsync(ph);

            if(ph.DataPageHeader == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            }

            int dataUsed = 0;
            int allValueCount = (int)_thriftColumnChunk.MetaData!.NumValues;
            int pageValueCount = ph.DataPageHeader.NumValues;

            if(_dataField.MaxRepetitionLevel > 0) {
                //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.

                int levelsRead = ReadLevels(
                    bytes.AsSpan(), _dataField.MaxRepetitionLevel,
                    pc.GetWriteableRepetitionLevelSpan(),
                    pageValueCount, null, out int usedLength);
                pc.MarkRepetitionLevels(levelsRead);
                dataUsed += usedLength;
            }

            int defNulls = 0;
            if(_dataField.MaxDefinitionLevel > 0) {
                int levelsRead = ReadLevels(
                    bytes.AsSpan().Slice(dataUsed), _dataField.MaxDefinitionLevel,
                    pc.GetWriteableDefinitionLevelSpan(),
                    pageValueCount, null, out int usedLength);
                dataUsed += usedLength;
                defNulls = pc.MarkDefinitionLevels(levelsRead, _dataField.MaxDefinitionLevel);
            }

            // try to be clever to detect how many elements to read
            int dataElementCount = pageValueCount - defNulls;

            ReadColumn(
                bytes.AsSpan().Slice(dataUsed),
                ph.DataPageHeader.Encoding,
                allValueCount, dataElementCount,
                pc);
        }

        private async Task ReadDataPageV2Async(PageHeader ph, PackedColumn pc, long maxValues) {
            if(ph.DataPageHeaderV2 == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            }

            using IronCompress.IronCompressResult bytes = await ReadPageDataV2Async(ph);
            int dataUsed = 0;

            if(_dataField.MaxRepetitionLevel > 0) {
                //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.
                int levelsRead = ReadLevels(bytes.AsSpan(),
                    _dataField.MaxRepetitionLevel, pc.GetWriteableRepetitionLevelSpan(),
                    ph.DataPageHeaderV2.NumValues, ph.DataPageHeaderV2.RepetitionLevelsByteLength, out int usedLength);
                dataUsed += usedLength;
                pc.MarkRepetitionLevels(levelsRead);
            }

            if(_dataField.MaxDefinitionLevel > 0) {
                int levelsRead = ReadLevels(bytes.AsSpan().Slice(dataUsed),
                    _dataField.MaxDefinitionLevel, pc.GetWriteableDefinitionLevelSpan(),
                    ph.DataPageHeaderV2.NumValues, ph.DataPageHeaderV2.DefinitionLevelsByteLength, out int usedLength);
                dataUsed += usedLength;
                pc.MarkDefinitionLevels(levelsRead);
            }

            int maxReadCount = ph.DataPageHeaderV2.NumValues - ph.DataPageHeaderV2.NumNulls;

            if((!(ph.DataPageHeaderV2.IsCompressed ?? false)) || _thriftColumnChunk.MetaData!.Codec == CompressionCodec.UNCOMPRESSED) {
                ReadColumn(bytes.AsSpan().Slice(dataUsed), ph.DataPageHeaderV2.Encoding, maxValues, maxReadCount, pc);
                return;
            }

            int dataSize = ph.CompressedPageSize - ph.DataPageHeaderV2.RepetitionLevelsByteLength -
                           ph.DataPageHeaderV2.DefinitionLevelsByteLength;

            int decompressedSize = ph.UncompressedPageSize - ph.DataPageHeaderV2.RepetitionLevelsByteLength -
                                   ph.DataPageHeaderV2.DefinitionLevelsByteLength;

            IronCompress.IronCompressResult decompressedDataByes = Compressor.Decompress(
                (CompressionMethod)(int)_thriftColumnChunk.MetaData.Codec,
                bytes.AsSpan().Slice(dataUsed),
                decompressedSize);

            ReadColumn(decompressedDataByes.AsSpan(),
                ph.DataPageHeaderV2.Encoding,
                maxValues, maxReadCount,
                pc);
        }

        private int ReadLevels(Span<byte> s, int maxLevel,
            Span<int> dest,
            int pageSize,
            int? length, out int usedLength) {

            int bitWidth = maxLevel.GetBitWidth();

            return RleBitpackedHybridEncoder.Decode(s, bitWidth, length, out usedLength, dest, pageSize);
        }

        private void ReadColumn(
            ReadOnlySpan<byte> src,
            Encoding encoding,
            long totalValuesInChunk,
            int totalValuesInPage,
            PackedColumn pc
        ) {
            // Bridge to APIs that require Span<byte>
            Span<byte> spanSrc = AsMutableSpan(src, out byte[] rented);
            try {
                switch(encoding) {
                    case Encoding.PLAIN: // 0
                    {
                            Array plainData = pc.GetPlainDataToReadInto(out int offset);
                            ParquetPlainEncoder.Decode(
                                plainData,
                                offset,
                                totalValuesInPage,
                                _schemaElement!,
                                spanSrc,
                                out int read);
                            pc.MarkUsefulPlainData(read);
                            break;
                        }

                    case Encoding.PLAIN_DICTIONARY: // 2
                    case Encoding.RLE_DICTIONARY:   // 8
                    {
                            Span<int> idxDest = pc.AllocateOrGetDictionaryIndexes(totalValuesInPage);
                            int indexCount = ReadRleDictionary(spanSrc, totalValuesInPage, idxDest);
                            pc.MarkUsefulDictionaryIndexes(indexCount);
                            pc.Checkpoint();
                            break;
                        }

                    case Encoding.RLE: // 3
                    {
                            Array plainData = pc.GetPlainDataToReadInto(out int offset);

                            if(_dataField.ClrType == typeof(bool)) {
                                // Decode to temp int[] then map to bool[]
                                int[] tmp = new int[plainData.Length];
                                int read = RleBitpackedHybridEncoder.Decode(
                                    spanSrc,
                                    _schemaElement!.TypeLength ?? 0,
                                    spanSrc.Length,
                                    out int _,
                                    tmp.AsSpan(offset),
                                    totalValuesInPage);

                                bool[] tgt = (bool[])plainData;
                                for(int i = 0; i < read; i++)
                                    tgt[i + offset] = tmp[i] == 1;

                                pc.MarkUsefulPlainData(read);
                                pc.Checkpoint();
                            } else {
                                Span<int> dest = ((int[])plainData).AsSpan(offset);
                                int read = RleBitpackedHybridEncoder.Decode(
                                    spanSrc,
                                    _schemaElement!.TypeLength ?? 0,
                                    spanSrc.Length,
                                    out int _,
                                    dest,
                                    totalValuesInPage);

                                pc.MarkUsefulPlainData(read);
                                pc.Checkpoint();
                            }
                            break;
                        }

                    case Encoding.DELTA_BINARY_PACKED: // 5
                    {
                            Array plainData = pc.GetPlainDataToReadInto(out int offset);
                            int read = DeltaBinaryPackedEncoder.Decode(
                                spanSrc,
                                plainData,
                                offset,
                                totalValuesInPage,
                                out _);
                            pc.MarkUsefulPlainData(read);
                            break;
                        }

                    case Encoding.DELTA_LENGTH_BYTE_ARRAY: // 6
                    {
                            Array plainData = pc.GetPlainDataToReadInto(out int offset);
                            int read = DeltaLengthByteArrayEncoder.Decode(
                                spanSrc,
                                plainData,
                                offset,
                                totalValuesInPage);
                            pc.MarkUsefulPlainData(read);
                            break;
                        }

                    case Encoding.DELTA_BYTE_ARRAY: // 7
                    {
                            Array plainData = pc.GetPlainDataToReadInto(out int offset);
                            int read = DeltaByteArrayEncoder.Decode(
                                spanSrc,
                                plainData,
                                offset,
                                totalValuesInPage);
                            pc.MarkUsefulPlainData(read);
                            break;
                        }

                    case Encoding.BIT_PACKED:        // 4 (deprecated)
                    case Encoding.BYTE_STREAM_SPLIT: // 9
                    default:
                        throw new ParquetException($"encoding {encoding} is not supported.");
                }
            } finally {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static int ReadRleDictionary(Span<byte> s, int maxReadCount, Span<int> dest) {
            int offset = 0;
            int destOffset = 0;
            int start = destOffset;
            int bitWidth = s[offset++];

            int length = s.Length - 1;

            //when bit width is zero reader must stop and just repeat zero maxValue number of times
            if(bitWidth == 0 || length == 0) {
                for(int i = 0; i < maxReadCount; i++) {
                    dest[destOffset++] = 0;
                }
            } else {
                if(length != 0) {
                    destOffset += RleBitpackedHybridEncoder.Decode(s.Slice(1), bitWidth, length, out int usedLength, dest, maxReadCount);
                }
            }

            return destOffset - start;
        }

        private static IronCompress.IronCompressResult DecompressWholePageFromBuffer(
            ReadOnlySpan<byte> pageBytes, int uncompressedSize, CompressionCodec codec) {
            if(codec == CompressionCodec.UNCOMPRESSED) {
                // mimic your ReadPageDataAsync contract using a rented buffer
                byte[] rented = ArrayPool<byte>.Shared.Rent(pageBytes.Length);
                pageBytes.CopyTo(rented);
                return new IronCompress.IronCompressResult(
                    rented, Codec.Snappy, false, pageBytes.Length, ArrayPool<byte>.Shared);
            }

            return Compressor.Decompress(
                (CompressionMethod)(int)codec, pageBytes, uncompressedSize);
        }

        private void ReadDictionaryPageFromBuffer(PageHeader ph, ReadOnlySpan<byte> pageBytes, PackedColumn pc) {
            if(pc.HasDictionary)
                throw new InvalidOperationException("dictionary already read");

            using IronCompress.IronCompressResult bytes =
                DecompressWholePageFromBuffer(pageBytes, ph.UncompressedPageSize, _thriftColumnChunk.MetaData!.Codec);

            Array dictionary = _dataField.CreateArray(ph.DictionaryPageHeader!.NumValues);
            ParquetPlainEncoder.Decode(dictionary, 0, ph.DictionaryPageHeader.NumValues,
                _schemaElement!, bytes.AsSpan(), out _);

            pc.AssignDictionary(dictionary);
        }

        private void ReadDataPageV1FromBuffer(PageHeader ph, ReadOnlySpan<byte> pageBytes, PackedColumn pc) {
            using IronCompress.IronCompressResult bytes =
                DecompressWholePageFromBuffer(pageBytes, ph.UncompressedPageSize, _thriftColumnChunk.MetaData!.Codec);

            if(ph.DataPageHeader == null)
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");

            int used = 0;
            int allValues = (int)_thriftColumnChunk.MetaData!.NumValues;
            int pageValues = ph.DataPageHeader.NumValues;

            if(_dataField.MaxRepetitionLevel > 0) {
                int n = ReadLevels(bytes.AsSpan(), _dataField.MaxRepetitionLevel,
                    pc.GetWriteableRepetitionLevelSpan(), pageValues, null, out int u);
                pc.MarkRepetitionLevels(n);
                used += u;
            }

            int defNulls = 0;
            if(_dataField.MaxDefinitionLevel > 0) {
                int n = ReadLevels(bytes.AsSpan().Slice(used), _dataField.MaxDefinitionLevel,
                    pc.GetWriteableDefinitionLevelSpan(), pageValues, null, out int u);
                used += u;
                defNulls = pc.MarkDefinitionLevels(n, _dataField.MaxDefinitionLevel);
            }

            int dataCount = pageValues - defNulls;
            ReadColumn(bytes.AsSpan().Slice(used), ph.DataPageHeader.Encoding, allValues, dataCount, pc);
        }

        private void ReadDataPageV2FromBuffer(PageHeader ph, ReadOnlySpan<byte> pageBytes, PackedColumn pc, long maxValues) {
            if(ph.DataPageHeaderV2 == null)
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");

            int used = 0;

            if(_dataField.MaxRepetitionLevel > 0) {
                int n = ReadLevels(pageBytes.ToArray(),
                    _dataField.MaxRepetitionLevel, pc.GetWriteableRepetitionLevelSpan(),
                    ph.DataPageHeaderV2.NumValues, ph.DataPageHeaderV2.RepetitionLevelsByteLength, out int u);
                used += u;
                pc.MarkRepetitionLevels(n);
            }

            if(_dataField.MaxDefinitionLevel > 0) {
                int n = ReadLevels(pageBytes.Slice(used).ToArray(),
                    _dataField.MaxDefinitionLevel, pc.GetWriteableDefinitionLevelSpan(),
                    ph.DataPageHeaderV2.NumValues, ph.DataPageHeaderV2.DefinitionLevelsByteLength, out int u);
                used += u;
                pc.MarkDefinitionLevels(n);
            }

            int maxRead = ph.DataPageHeaderV2.NumValues - ph.DataPageHeaderV2.NumNulls;

            bool notCompressed =
                (!(ph.DataPageHeaderV2.IsCompressed ?? false)) ||
                _thriftColumnChunk.MetaData!.Codec == CompressionCodec.UNCOMPRESSED;

            if(notCompressed) {
                ReadColumn(pageBytes.Slice(used).ToArray(), ph.DataPageHeaderV2.Encoding, maxValues, maxRead, pc);
                return;
            }

            int dataSize = ph.CompressedPageSize
                - ph.DataPageHeaderV2.RepetitionLevelsByteLength
                - ph.DataPageHeaderV2.DefinitionLevelsByteLength;

            int decompSize = ph.UncompressedPageSize
                - ph.DataPageHeaderV2.RepetitionLevelsByteLength
                - ph.DataPageHeaderV2.DefinitionLevelsByteLength;

            ColumnMetaData meta = _thriftColumnChunk.MetaData
                ?? throw new InvalidDataException("ColumnChunk.MetaData is missing");

            using IronCompress.IronCompressResult decomp = Compressor.Decompress(
                (CompressionMethod)(int)meta.Codec,
                pageBytes.Slice(used, dataSize),
                decompSize);

            ReadColumn(decomp.AsSpan(), ph.DataPageHeaderV2.Encoding, maxValues, maxRead, pc);
        }

    }
}
