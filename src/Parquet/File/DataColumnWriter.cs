using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.File; 
class DataColumnWriter {
    private readonly Stream _stream;
    private readonly ThriftFooter _footer;
    private readonly SchemaElement _schemaElement;
    private readonly CompressionMethod _compressionMethod;
    private readonly CompressionLevel _compressionLevel;
    private readonly Dictionary<string, string>? _keyValueMetadata;
    private readonly ParquetOptions _options;
    private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();

    public DataColumnWriter(
       Stream stream,
       ThriftFooter footer,
       SchemaElement schemaElement,
       CompressionMethod compressionMethod,
       ParquetOptions options,
       CompressionLevel compressionLevel,
       Dictionary<string, string>? keyValueMetadata) {
        _stream = stream;
        _footer = footer;
        _schemaElement = schemaElement;
        _compressionMethod = compressionMethod;
        _compressionLevel = compressionLevel;
        _keyValueMetadata = keyValueMetadata;
        _options = options;
        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;
    }

    public async Task<ColumnChunk> WriteAsync(
        FieldPath fullPath, DataColumn column,
        CancellationToken cancellationToken = default) {

        // Num_values in the chunk does include null values - I have validated this by dumping spark-generated file.
        ColumnChunk chunk = _footer.CreateColumnChunk(
            _compressionMethod, _stream, _schemaElement.Type!.Value, fullPath, column.NumValues,
            _keyValueMetadata);
        if(chunk.MetaData == null)
            throw new InvalidDataException($"{nameof(chunk.MetaData)} can not be null");

        ColumnMetrics metrics = await WriteColumnAsync(
            chunk, column, _schemaElement,
            cancellationToken);
        chunk.MetaData.Encodings = metrics.GetUsedEncodings();

        //generate stats for column chunk
        chunk.MetaData.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);

        //the following counters must include both data size and header size
        chunk.MetaData.TotalCompressedSize = metrics.CompressedSize;
        chunk.MetaData.TotalUncompressedSize = metrics.UncompressedSize;

        return chunk;
    }

    class ColumnMetrics {
        public int CompressedSize;
        public int UncompressedSize;
        public readonly List<PageHeader> Pages = new();

        public List<Encoding> GetUsedEncodings() {
            var r = new HashSet<Encoding>();
            foreach(PageHeader page in Pages) {
                if(page.DictionaryPageHeader != null) {
                    r.Add(page.DictionaryPageHeader.Encoding);
                }
                if(page.DataPageHeader != null) {
                    r.Add(page.DataPageHeader.Encoding);
                    r.Add(page.DataPageHeader.DefinitionLevelEncoding);
                    r.Add(page.DataPageHeader.RepetitionLevelEncoding);
                }
                if(page.DataPageHeaderV2 != null) {
                    r.Add(page.DataPageHeaderV2.Encoding);
                }
            }
            return r.ToList();
        }
    }

    private async Task CompressAndWriteAsync(
        PageHeader ph, MemoryStream uncompressedData,
        ColumnMetrics cs,
        CancellationToken cancellationToken) {

        int uncompressedLength = (int)uncompressedData.Length;
        using IMemoryOwner<byte> pageData = await Compressor.Instance.CompressAsync(
            _compressionMethod, _compressionLevel, uncompressedData);
        int compressedLength = pageData.Memory.Length;

        ph.UncompressedPageSize = uncompressedLength;
        ph.CompressedPageSize = compressedLength;

        //write the header in
        using(MemoryStream headerMs = _rmsMgr.GetStream()) {
            ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
            int headerSize = (int)headerMs.Length;
            headerMs.Position = 0;
            _stream.Flush();

            // write header
            await headerMs.CopyToAsync(_stream);

            cs.CompressedSize += headerSize;
            cs.UncompressedSize += headerSize;
        }

        // write data
        await pageData.Memory.CopyToAsync(_stream);

        cs.CompressedSize += ph.CompressedPageSize;
        cs.UncompressedSize += ph.UncompressedPageSize;
    }

    private async Task<ColumnMetrics> WriteColumnAsync(ColumnChunk chunk, DataColumn column,
       SchemaElement tse,
       CancellationToken cancellationToken = default) {

        column.Field.EnsureAttachedToSchema(nameof(column));

        var r = new ColumnMetrics();

        /*
         * Page header must preceeed actual data (compressed or not) however it contains both
         * the uncompressed and compressed data size which we don't know! This somehow limits
         * the write efficiency.
         */

        using var pc = new PackedColumn(column);
        pc.Pack(_options.UseDictionaryEncoding, _options.DictionaryEncodingThreshold);

        // dictionary page
        if(pc.HasDictionary) {
            PageHeader ph = _footer.CreateDictionaryPage(pc.Dictionary!.Length, out _);
            r.Pages.Add(ph);
            using MemoryStream ms = _rmsMgr.GetStream();
            ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                   tse,
                   ms, column.Statistics);

            await CompressAndWriteAsync(ph, ms, r, cancellationToken);
        }

        // data page
        using(MemoryStream ms = _rmsMgr.GetStream()) {
            Array data = pc.GetPlainData(out int offset, out int count);
            bool deltaEncode = column.IsDeltaEncodable && _options.UseDeltaBinaryPackedEncoding && DeltaBinaryPackedEncoder.CanEncode(data, offset, count);
           
            // data page Num_values also does include NULLs
            PageHeader ph = _footer.CreateDataPage(column.NumValues, pc.HasDictionary, deltaEncode, out DataPageHeader dph);
            r.Pages.Add(ph);
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
                if(deltaEncode) {
                    DeltaBinaryPackedEncoder.Encode(data, offset, count, ms, column.Statistics);
                } else {
                    ParquetPlainEncoder.Encode(data, offset, count, tse, ms, pc.HasDictionary ? null : column.Statistics);
                }
            }

            dph.Statistics = column.Statistics.ToThriftStatistics(tse);
            await CompressAndWriteAsync(ph, ms, r, cancellationToken);
        }

        return r;
    }

    private static void WriteLevels(Stream s, Span<int> levels, int count, int maxValue) {
        int bitWidth = maxValue.GetBitWidth();
        RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.Slice(0, count));
    }
}
