using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.File;

class DataColumnWriter {
    private readonly Stream _stream;
    private readonly ThriftFooter _footer;
    private readonly SchemaElement _schemaElement;
    private readonly Dictionary<string, string>? _keyValueMetadata;
    private readonly ParquetOptions _options;
    private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();

    public DataColumnWriter(
       Stream stream,
       ThriftFooter footer,
       SchemaElement schemaElement,
       ParquetOptions options,
       Dictionary<string, string>? keyValueMetadata) {
        _stream = stream;
        _footer = footer;
        _schemaElement = schemaElement;
        _keyValueMetadata = keyValueMetadata;
        _options = options;
        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;
    }

    public async Task<ColumnChunk> WriteAsync<T>(
        FieldPath fullPath,
        WritingColumn<T> wc,
        CancellationToken cancellationToken) where T : struct {
        // Num_values in the chunk does include null values - I have validated this by dumping spark-generated file.
        ColumnChunk chunk = _footer.CreateColumnChunk(
            _options.CompressionMethod, _stream, _schemaElement.Type!.Value, fullPath, wc.NumValues,
            _keyValueMetadata);
        if(chunk.MetaData == null)
            throw new InvalidDataException($"{nameof(chunk.MetaData)} can not be null");

        ColumnMetrics metrics = await WriteAsync(
            chunk, wc, _schemaElement,
            cancellationToken);
        chunk.MetaData.Encodings = metrics.GetUsedEncodings();

        //generate stats for column chunk
        chunk.MetaData.Statistics = wc.Statistics.ToThriftStatistics(_schemaElement);

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
            _options.CompressionMethod, _options.CompressionLevel, uncompressedData);
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

    private async Task<ColumnMetrics> WriteAsync<T>(ColumnChunk chunk,
        WritingColumn<T> wc,
        SchemaElement tse,
        CancellationToken cancellationToken) where T : struct {

        wc.Field.EnsureAttachedToSchema(nameof(wc.Field));
        wc.Pack(_options);

        var r = new ColumnMetrics();

        /*
         * Page header must preceeed actual data (compressed or not) however it contains both
         * the uncompressed and compressed data size which we don't know! This somehow limits
         * the write efficiency.
         */

        // dictionary page
        if(wc.HasDictionary) {
            PageHeader ph = _footer.CreateDictionaryPage(wc.Dictionary.Length, out _);
            r.Pages.Add(ph);
            using MemoryStream ms = _rmsMgr.GetStream();
            ParquetPlainEncoder.Encode(wc.Dictionary, ms, tse, wc.Statistics);
            await CompressAndWriteAsync(ph, ms, r, cancellationToken);
        }

        // data page
        using(MemoryStream ms = _rmsMgr.GetStream()) {
            bool deltaEncode = wc.Field.IsDeltaEncodable && _options.GetEncodingHint(wc.Field) == EncodingHint.DeltaBinaryPacked && DeltaBinaryPackedEncoder.CanEncode(wc.Values);

            // data page Num_values also does include NULLs
            PageHeader ph = _footer.CreateDataPage(wc.NumValues, wc.HasDictionary, deltaEncode, out DataPageHeader dph);
            r.Pages.Add(ph);

            if(wc.HasRepetitionLevels) {
                WriteLevels(ms, wc.RepetitionLevels!, wc.Field.MaxRepetitionLevel);
            }
            if(wc.HasDefinitionLevels) {
                WriteLevels(ms, wc.DefinitionLevels!, wc.Field.MaxDefinitionLevel);
            }

            if(wc.HasDictionary) {
                // dictionary indexes are always encoded with RLE
                int bitWidth = wc.Dictionary.Length.GetBitWidth();  // bit width is determined by the dictionary size
                ms.WriteByte((byte)bitWidth);   // bit width is stored as 1 byte before encoded data
                RleBitpackedHybridEncoder.Encode(ms, wc.DictionaryIndexes, bitWidth);
            } else {
                if(deltaEncode) {
                    DeltaBinaryPackedEncoder.Encode(wc.Values, ms, wc.Statistics);
                } else {
                    ParquetPlainEncoder.Encode(wc.Values,
                        ms,
                        tse,
                        wc.HasDictionary ? null : wc.Statistics);
                }
            }

            dph.Statistics = wc.Statistics.ToThriftStatistics(tse);
            await CompressAndWriteAsync(ph, ms, r, cancellationToken);
        }

        return r;
    }

    private static void WriteLevels(Stream s, ReadOnlySpan<int> levels, int maxValue) {
        int bitWidth = maxValue.GetBitWidth();
        RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels);
    }
}
