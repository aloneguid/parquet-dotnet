using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance.Buffers;
using Microsoft.IO;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Encryption;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Schema;

namespace Parquet.File;

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
    private readonly CompressionMethod _compressionMethod;
    private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();
    private readonly ParquetCryptoContext? _cryptoContext;
    private readonly short _rowGroupOrdinal;
    private readonly short _columnOrdinal;

    internal DataColumnReader(
       DataField dataField,
       Stream inputStream,
       ColumnChunk thriftColumnChunk,
       DataColumnStatistics? stats,
       ThriftFooter footer,
       ParquetOptions? parquetOptions,
       ParquetCryptoContext? cryptoContext,
       short rowGroupOrdinal,
       short columnOrdinal) {
        _dataField = dataField ?? throw new ArgumentNullException(nameof(dataField));
        _inputStream = inputStream ?? throw new ArgumentNullException(nameof(inputStream));
        _thriftColumnChunk = thriftColumnChunk ?? throw new ArgumentNullException(nameof(thriftColumnChunk));
        _stats = stats;
        _compressionMethod = (CompressionMethod)(int)(thriftColumnChunk.MetaData?.Codec ?? CompressionCodec.UNCOMPRESSED);
        _footer = footer ?? throw new ArgumentNullException(nameof(footer));
        _options = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));
        _cryptoContext = cryptoContext;
        _rowGroupOrdinal = rowGroupOrdinal;
        _columnOrdinal = columnOrdinal;

        dataField.EnsureAttachedToSchema(nameof(dataField));

        _schemaElement = _footer.GetSchemaElement(_thriftColumnChunk);

        // parquetOptions is guaranteed non-null due to earlier null check.
        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = parquetOptions.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = parquetOptions.MaximumLargePoolFreeBytes;
    }

    /// <summary>
    /// Return data column statistics
    /// </summary>
    /// <returns>Data column statistics or null</returns>
    public DataColumnStatistics? GetColumnStatistics() => _stats;

    public async ValueTask ReadAsync<T>(ReadingColumn<T> rc, CancellationToken cancellationToken) where T : struct {
        // how many values are in column chunk, as there may be multiple data pages
        int totalValuesInChunk = (int)_thriftColumnChunk.MetaData!.NumValues;
        int definedValuesCount = totalValuesInChunk;
        if(_stats?.NullCount != null)
            definedValuesCount -= (int)_stats.NullCount.Value;

        long fileOffset = GetFileOffset();
        long pageOffset = fileOffset;
        short pageOrdinal = 0;

        while(rc.ValuesRead < totalValuesInChunk) {
            // use absolute positioning on every page read, because in some edge cases page reader may not exhaust or over-read page data
            _inputStream.Seek(pageOffset, SeekOrigin.Begin);
            bool dictionaryPage = _thriftColumnChunk.MetaData?.DictionaryPageOffset == pageOffset;
            PageHeader ph = ReadPageHeader(dictionaryPage, pageOrdinal);
            pageOffset = _inputStream.Position + ph.CompressedPageSize;

            switch(ph.Type) {
                case PageType.DICTIONARY_PAGE:
                    await ReadDictionaryPageAsync(ph, rc, cancellationToken);
                    break;
                case PageType.DATA_PAGE:
                    await ReadDataPageV1Async(ph, rc, pageOrdinal, cancellationToken);
                    pageOrdinal = checked((short)(pageOrdinal + 1));
                    break;
                case PageType.DATA_PAGE_V2:
                    await ReadDataPageV2Async(ph, rc, totalValuesInChunk, pageOrdinal, cancellationToken);
                    pageOrdinal = checked((short)(pageOrdinal + 1));
                    break;
                default:
                    throw new NotSupportedException($"can't read page type {ph.Type}");
            }
        }
    }

    internal async ValueTask<(OffsetIndex OffsetIndex, ColumnIndex? ColumnIndex)> ScanPageIndexesAsync(
        CancellationToken cancellationToken = default) {
        if(!_inputStream.CanSeek)
            throw new InvalidOperationException("Input stream must be seekable to scan page indexes.");

        long originalPosition = _inputStream.Position;
        try {
            int totalValuesInChunk = checked((int)_thriftColumnChunk.MetaData!.NumValues);
            int valuesRead = 0;
            long firstRowIndex = 0;
            long pageOffset = GetFileOffset();
            short pageOrdinal = 0;
            var pageLocations = new List<PageLocation>();
            var nullPages = new List<bool>();
            var minValues = new List<byte[]>();
            var maxValues = new List<byte[]>();
            var nullCounts = new List<long>();
            bool canBuildColumnIndex = true;
            bool canBuildNullCounts = true;

            while(valuesRead < totalValuesInChunk) {
                cancellationToken.ThrowIfCancellationRequested();
                long pageStart = pageOffset;
                _inputStream.Seek(pageStart, SeekOrigin.Begin);
                bool dictionaryPage = _thriftColumnChunk.MetaData.DictionaryPageOffset == pageStart;
                PageHeader pageHeader = ReadPageHeader(dictionaryPage, pageOrdinal);
                pageOffset = checked(_inputStream.Position + pageHeader.CompressedPageSize);

                if(pageHeader.Type == PageType.DICTIONARY_PAGE)
                    continue;

                int valueCount;
                int rowCount;
                Statistics? statistics;
                switch(pageHeader.Type) {
                    case PageType.DATA_PAGE:
                        DataPageHeader dataPageHeader = pageHeader.DataPageHeader
                            ?? throw new InvalidDataException("Data page V1 header is missing.");
                        valueCount = dataPageHeader.NumValues;
                        rowCount = await GetDataPageV1RowCountAsync(
                            pageHeader,
                            dataPageHeader.NumValues,
                            pageOrdinal);
                        statistics = dataPageHeader.Statistics;
                        break;
                    case PageType.DATA_PAGE_V2:
                        DataPageHeaderV2 dataPageHeaderV2 = pageHeader.DataPageHeaderV2
                            ?? throw new InvalidDataException("Data page V2 header is missing.");
                        valueCount = dataPageHeaderV2.NumValues;
                        rowCount = dataPageHeaderV2.NumRows;
                        statistics = dataPageHeaderV2.Statistics;
                        break;
                    default:
                        throw new InvalidDataException($"Expected a data page, found '{pageHeader.Type}'.");
                }

                if(valueCount <= 0 || rowCount < 0)
                    throw new InvalidDataException("Page header contains invalid value or row counts.");

                pageLocations.Add(new PageLocation {
                    Offset = pageStart,
                    CompressedPageSize = checked((int)(pageOffset - pageStart)),
                    FirstRowIndex = firstRowIndex
                });
                valuesRead = checked(valuesRead + valueCount);
                firstRowIndex = checked(firstRowIndex + rowCount);

                byte[]? minValue = statistics?.MinValue ?? statistics?.Min;
                byte[]? maxValue = statistics?.MaxValue ?? statistics?.Max;
                bool nullPage = statistics?.NullCount == valueCount;
                if(statistics?.NullCount == null) {
                    canBuildNullCounts = false;
                } else {
                    nullCounts.Add(statistics.NullCount.Value);
                }
                if(statistics == null || (!nullPage && (minValue == null || maxValue == null))) {
                    canBuildColumnIndex = false;
                } else {
                    nullPages.Add(nullPage);
                    minValues.Add(nullPage ? Array.Empty<byte>() : minValue!);
                    maxValues.Add(nullPage ? Array.Empty<byte>() : maxValue!);
                }

                pageOrdinal = checked((short)(pageOrdinal + 1));
            }

            if(valuesRead != totalValuesInChunk)
                throw new InvalidDataException("Page value counts do not match the column chunk metadata.");

            var offsetIndex = new OffsetIndex { PageLocations = pageLocations };
            ColumnIndex? columnIndex = canBuildColumnIndex
                ? new ColumnIndex {
                    NullPages = nullPages,
                    MinValues = minValues,
                    MaxValues = maxValues,
                    BoundaryOrder = BoundaryOrder.UNORDERED,
                    NullCounts = canBuildNullCounts ? nullCounts : null
                }
                : null;
            return (offsetIndex, columnIndex);
        } finally {
            _inputStream.Seek(originalPosition, SeekOrigin.Begin);
        }
    }

    private PageHeader ReadPageHeader(bool dictionaryPage, short pageOrdinal) {
        if(_cryptoContext == null)
            return PageHeader.Read(new ThriftCompactProtocolReader(_inputStream));

        byte[] header = dictionaryPage
            ? _cryptoContext.Decrypt(
                _inputStream,
                ParquetModuleType.DictionaryPageHeader,
                _rowGroupOrdinal,
                _columnOrdinal)
            : _cryptoContext.Decrypt(
                _inputStream,
                ParquetModuleType.DataPageHeader,
                _rowGroupOrdinal,
                _columnOrdinal,
                pageOrdinal);
        using var headerStream = new MemoryStream(header, writable: false);
        PageHeader pageHeader = PageHeader.Read(new ThriftCompactProtocolReader(headerStream));
        ParquetCryptoContext.ValidateTrailingPadding(header, headerStream.Position, "encrypted page header");
        return pageHeader;
    }

    private async ValueTask<int> GetDataPageV1RowCountAsync(
        PageHeader pageHeader,
        int valueCount,
        short pageOrdinal) {
        if(_dataField.MaxRepetitionLevel == 0)
            return valueCount;

        using IMemoryOwner<byte> bytes = await ReadPageDataAsync(
            pageHeader,
            ParquetModuleType.DataPage,
            pageOrdinal);
        var repetitionLevels = new int[valueCount];
        int levelsRead = ReadLevels(
            bytes.Memory.Span,
            _dataField.MaxRepetitionLevel,
            repetitionLevels,
            valueCount,
            null,
            out _);
        int rowCount = 0;
        for(int index = 0; index < levelsRead; index++) {
            if(repetitionLevels[index] == 0)
                rowCount++;
        }
        return rowCount;
    }

    private async ValueTask<IMemoryOwner<byte>> ReadPageDataAsync(
        PageHeader ph,
        ParquetModuleType module,
        short? pageOrdinal = null) {
        Stream src;
        if(_cryptoContext == null) {
            src = _inputStream.Sub(_inputStream.Position, ph.CompressedPageSize);
        } else {
            long start = _inputStream.Position;
            byte[] encryptedPage = _cryptoContext.Decrypt(
                _inputStream,
                module,
                _rowGroupOrdinal,
                _columnOrdinal,
                pageOrdinal);
            if(_inputStream.Position - start != ph.CompressedPageSize)
                throw new InvalidDataException("The encrypted page size does not match its header.");
            src = new MemoryStream(encryptedPage, writable: false);
        }
        return await Compressor.Instance.Decompress(_compressionMethod, src, ph.UncompressedPageSize);
    }

    private async ValueTask ReadDictionaryPageAsync<T>(PageHeader ph, ReadingColumn<T> rc, CancellationToken cancellationToken) where T : struct {

        if(rc.HasDictionary)
            throw new InvalidOperationException("dictionary already read");

        //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.
        using IMemoryOwner<byte> bytes = await ReadPageDataAsync(ph, ParquetModuleType.DictionaryPage);

        // Dictionary should not contain null values
        Span<T> dictionary = rc.AllocateDictionary(ph.DictionaryPageHeader!.NumValues);

        ParquetPlainEncoder.Decode(dictionary, ph.DictionaryPageHeader.NumValues,
               _schemaElement!, bytes.Memory.Span, out int dictionaryOffset);
    }

    private long GetFileOffset() =>
        // get the minimum offset, we'll just read pages in sequence as DictionaryPageOffset/Data_page_offset are not reliable
        new[]
            {
                _thriftColumnChunk.MetaData?.DictionaryPageOffset ?? 0,
                _thriftColumnChunk.MetaData!.DataPageOffset,
                _thriftColumnChunk.MetaData?.IndexPageOffset ?? 0
            }
            .Where(e => e != 0)
            .Min();

    private async ValueTask ReadDataPageV1Async<T>(
        PageHeader ph,
        ReadingColumn<T> rc,
        short pageOrdinal,
        CancellationToken cancellationToken) where T : struct {
        using IMemoryOwner<byte> bytes = await ReadPageDataAsync(ph, ParquetModuleType.DataPage, pageOrdinal);

        if(ph.DataPageHeader == null) {
            throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
        }

        int dataUsed = 0;
        int allValueCount = (int)_thriftColumnChunk.MetaData!.NumValues;
        int pageValueCount = ph.DataPageHeader.NumValues;

        if(_dataField.MaxRepetitionLevel > 0) {
            int levelsRead = ReadLevels(
                bytes.Memory.Span, _dataField.MaxRepetitionLevel,
                rc.RepetitionLevelsToReadInto,
                pageValueCount, null, out int usedLength);
            rc.MarkRepetitionLevels(levelsRead);
            dataUsed += usedLength;
        }

        int defNulls = 0;
        if(_dataField.MaxDefinitionLevel > 0) {
            int levelsRead = ReadLevels(
                bytes.Memory.Span.Slice(dataUsed), _dataField.MaxDefinitionLevel,
                rc.DefinitionLevelsToReadInto,
                pageValueCount, null, out int usedLength);
            dataUsed += usedLength;
            defNulls = rc.MarkDefinitionLevels(levelsRead, _dataField.MaxDefinitionLevel);
        }

        // try to be clever to detect how many elements to read
        int dataElementCount = pageValueCount - defNulls;

        ReadColumn(
            bytes.Memory.Span.Slice(dataUsed),
            ph.DataPageHeader.Encoding,
            allValueCount, dataElementCount,
            rc);
    }

    private async ValueTask ReadDataPageV2Async<T>(
        PageHeader ph,
        ReadingColumn<T> rc,
        long maxValues,
        short pageOrdinal,
        CancellationToken cancellationToken) where T : struct {
        if(ph.DataPageHeaderV2 == null) {
            throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
        }

        byte[]? decryptedPage = null;
        if(_cryptoContext != null) {
            long start = _inputStream.Position;
            decryptedPage = _cryptoContext.Decrypt(
                _inputStream,
                ParquetModuleType.DataPage,
                _rowGroupOrdinal,
                _columnOrdinal,
                pageOrdinal);
            if(_inputStream.Position - start != ph.CompressedPageSize)
                throw new InvalidDataException("The encrypted page size does not match its header.");
        }
        int pageLength = decryptedPage?.Length ?? ph.CompressedPageSize;
        using var pageMemory = MemoryOwner<byte>.Allocate(pageLength);
        if(decryptedPage != null) {
            decryptedPage.CopyTo(pageMemory.Span);
        } else {
            await using Stream src = _inputStream.Sub(_inputStream.Position, ph.CompressedPageSize);
            await src.CopyToAsync(pageMemory.Memory, cancellationToken);
        }
        int dataUsed = 0;

        if(_dataField.MaxRepetitionLevel > 0) {
            int levelsRead = ReadLevels(pageMemory.Span,
                _dataField.MaxRepetitionLevel, rc.RepetitionLevelsToReadInto,
                ph.DataPageHeaderV2.NumValues, ph.DataPageHeaderV2.RepetitionLevelsByteLength, out int usedLength);
            dataUsed += usedLength;
            rc.MarkRepetitionLevels(levelsRead);

            throw new NotImplementedException();
        }

        if(_dataField.MaxDefinitionLevel > 0) {
            int levelsRead = ReadLevels(
                pageMemory.Memory.Span.Slice(dataUsed), _dataField.MaxDefinitionLevel,
                rc.DefinitionLevelsToReadInto,
                ph.DataPageHeaderV2.NumValues, ph.DataPageHeaderV2.DefinitionLevelsByteLength, out int usedLength);
            dataUsed += usedLength;
            rc.MarkDefinitionLevels(levelsRead, _dataField.MaxDefinitionLevel);
        }

        int maxReadCount = ph.DataPageHeaderV2.NumValues - ph.DataPageHeaderV2.NumNulls;

        if(ph.DataPageHeaderV2.IsCompressed == false || _thriftColumnChunk.MetaData!.Codec == CompressionCodec.UNCOMPRESSED) {
            ReadColumn(pageMemory.Span.Slice(dataUsed), ph.DataPageHeaderV2.Encoding, maxValues, maxReadCount, rc);
            return;
        }

        int dataSize = ph.CompressedPageSize - ph.DataPageHeaderV2.RepetitionLevelsByteLength -
                       ph.DataPageHeaderV2.DefinitionLevelsByteLength;

        int decompressedSize = ph.UncompressedPageSize - ph.DataPageHeaderV2.RepetitionLevelsByteLength -
                               ph.DataPageHeaderV2.DefinitionLevelsByteLength;


        // decompress into rented memory
        using IMemoryOwner<byte> decompressedDataMemory = await Compressor.Instance.Decompress(
            _compressionMethod,
            pageMemory.Memory.Sub(dataUsed, pageMemory.Length - dataUsed),
            ph.UncompressedPageSize);

        ReadColumn(decompressedDataMemory.Memory.Span,
            ph.DataPageHeaderV2.Encoding,
            maxValues, maxReadCount,
            rc);
    }

    private int ReadLevels(Span<byte> s, int maxLevel,
        Span<int> dest,
        int pageSize,
        int? length, out int usedLength) {

        int bitWidth = maxLevel.GetBitWidth();

        return RleBitpackedHybridEncoder.Decode(s, bitWidth, length, out usedLength, dest, pageSize);
    }

    private void ReadColumn<T>(Span<byte> src,
        Encoding encoding, long totalValuesInChunk, int totalValuesInPage,
        ReadingColumn<T> rc)
        where T : struct {
        //dictionary encoding uses RLE to encode data

        switch(encoding) {
            case Encoding.PLAIN: { // 0
                    ParquetPlainEncoder.Decode(rc.ValuesToReadInto,
                        totalValuesInPage,
                        _schemaElement!, src, out int read);
                    rc.MarkValuesRead(read);
                }
                break;

            case Encoding.PLAIN_DICTIONARY: // 2  // values are still encoded in RLE
            case Encoding.RLE_DICTIONARY: { // 8
                    Span<int> span = rc.AllocateOrGetDictionaryIndexes(totalValuesInPage);
                    int indexCount = ReadRleDictionary(src, totalValuesInPage, span);
                    rc.MarkDictionaryIndexesRead(indexCount);
                    rc.Checkpoint();
                }
                break;

            case Encoding.RLE: { // 3
                    if(_dataField.ClrType == typeof(bool)) {
                        // for boolean values, we need to read into temporary int buffer and convert to booleans.
                        // todo: we can optimise this by implementing boolean RLE decoder
                        Span<bool> dest = rc.ValuesToReadInto.AsSpan<T, bool>();

                        int[] tmp = new int[dest.Length];
                        int read = RleBitpackedHybridEncoder.Decode(src,
                            _schemaElement!.TypeLength ?? 0,
                            src.Length, out int usedLength, tmp.AsSpan(), totalValuesInPage);

                        // copy back to bool array
                        for(int i = 0; i < read; i++) {
                            dest[i] = tmp[i] == 1;
                        }

                        rc.MarkValuesRead(read);
                    } else {
                        Span<int> dest = rc.ValuesToReadInto.AsSpan<T, int>();
                        int read = RleBitpackedHybridEncoder.Decode(src,
                            _schemaElement!.TypeLength ?? 0,
                            src.Length, out int usedLength, dest, totalValuesInPage);
                        rc.MarkValuesRead(read);
                    }
                }
                break;

            case Encoding.DELTA_BINARY_PACKED: {// 5
                    int read = DeltaBinaryPackedEncoder.Decode(src, rc.ValuesToReadInto, totalValuesInPage, out _);
                    rc.MarkValuesRead(read);
                }
                break;

            case Encoding.DELTA_LENGTH_BYTE_ARRAY: {  // 6
                    int read = DeltaLengthByteArrayEncoder.Decode(src, rc.ValuesToReadInto, totalValuesInPage);
                    rc.MarkValuesRead(read);
                }
                break;

            case Encoding.DELTA_BYTE_ARRAY: {         // 7
                    int read = DeltaByteArrayEncoder.Decode(src, rc.ValuesToReadInto, totalValuesInPage);
                    rc.MarkValuesRead(read);
                }
                break;

            case Encoding.BYTE_STREAM_SPLIT: {       // 9
                    ByteStreamSplitEncoder.Decode(src, rc.ValuesToReadInto.Slice(0, totalValuesInPage));
                    rc.MarkValuesRead(totalValuesInPage);
                }
                break;
            case Encoding.BIT_PACKED:                // 4 (deprecated)
            default:
                throw new ParquetException($"encoding {encoding} is not supported.");
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
}
