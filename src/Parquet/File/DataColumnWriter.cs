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
    private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();

    public static async Task<ColumnChunk> WriteAsync(
        FieldPath fullPath, DataColumn column,
        Stream stream,
       SchemaElement schemaElement,
       CompressionMethod compressionMethod,
       ParquetOptions options,
       CompressionLevel compressionLevel,
       Dictionary<string, string>? keyValueMetadata,
       OrderedCommitGate orderedCommitGate,
       CancellationToken cancellationToken = default) {
        long startPos = stream.Position;

        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;

        ColumnMetrics metrics = await WriteColumnAsync(stream, column, schemaElement, options, compressionMethod, compressionLevel, orderedCommitGate, cancellationToken);

        //generate stats for column chunk
        Statistics statistics = column.Statistics.ToThriftStatistics(schemaElement);

        // Num_values in the chunk does include null values - I have validated this by dumping spark-generated file.
        ColumnChunk chunk = ThriftFooter.CreateColumnChunk(
            compressionMethod, startPos, schemaElement.Type!.Value, fullPath, column.NumValues,
            keyValueMetadata, statistics, metrics);

        return chunk;
    }

    readonly record struct CompressResult((int, int) ColumnSizes, MemoryStream HeaderMs, IMemoryOwner<byte> PageData);

    private static async Task<CompressResult> CompressAsync(PageHeader ph, MemoryStream uncompressedData, CompressionMethod compressionMethod, CompressionLevel compressionLevel,
        CancellationToken cancellationToken) {

        int uncompressedLength = (int)uncompressedData.Length;
        IMemoryOwner<byte> pageData = await Compressor.Instance.CompressAsync(
            compressionMethod, compressionLevel, uncompressedData);
        int compressedLength = pageData.Memory.Length;

        ph.UncompressedPageSize = uncompressedLength;
        ph.CompressedPageSize = compressedLength;

        int headerSize;

        //write the header in
        MemoryStream headerMs = _rmsMgr.GetStream();
        ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
        headerSize = (int)headerMs.Length;
        headerMs.Position = 0;

        return new((ph.CompressedPageSize + headerSize, ph.UncompressedPageSize + headerSize), headerMs, pageData);
    }

    private static async Task WriteAsync(Stream stream, CompressResult compressResult) {
        stream.Flush();

        await compressResult.HeaderMs.CopyToAsync(stream);
        await compressResult.PageData.Memory.CopyToAsync(stream);
    }


    private static async Task<ColumnMetrics> WriteColumnAsync(Stream stream,
        DataColumn column,
       SchemaElement tse, ParquetOptions options, CompressionMethod compressionMethod, CompressionLevel compressionLevel,
OrderedCommitGate orderedCommitGate, CancellationToken cancellationToken = default) {

        OrderedCommitGate.Token commitToken = orderedCommitGate.QueueForCommit();
        await Task.Yield();

        column.Field.EnsureAttachedToSchema(nameof(column));

        var r = new ColumnMetrics();

        /*
         * Page header must preceeed actual data (compressed or not) however it contains both
         * the uncompressed and compressed data size which we don't know! This somehow limits
         * the write efficiency.
         */

        using var pc = new PackedColumn(column);
        pc.Pack(options.UseDictionaryEncoding, options.DictionaryEncodingThreshold);

        // dictionary page
        (CompressResult dictCompressResult, MemoryStream ms)? dictWriteState = null;
        try {
            // dictionary page
            if(pc.HasDictionary) {
                PageHeader ph = ThriftFooter.CreateDictionaryPage(pc.Dictionary!.Length, out _);
                r = r.WithAddedPage(ph);
                MemoryStream ms = _rmsMgr.GetStream();
                ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                       tse,
                       ms, column.Statistics);

                CompressResult result = await CompressAsync(ph, ms, compressionMethod, compressionLevel, cancellationToken);
                r = r.WithAddedSizes(result.ColumnSizes);
                dictWriteState = (result, ms);
            }

            // data page
            using(MemoryStream ms = _rmsMgr.GetStream()) {
                Array data = pc.GetPlainData(out int offset, out int count);
                bool deltaEncode = column.IsDeltaEncodable && options.UseDeltaBinaryPackedEncoding && DeltaBinaryPackedEncoder.CanEncode(data, offset, count);


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

                Statistics statistics = column.Statistics.ToThriftStatistics(tse);

                // data page Num_values also does include NULLs
                PageHeader ph = ThriftFooter.CreateDataPage(column.NumValues, pc.HasDictionary, deltaEncode, statistics);
                r = r.WithAddedPage(ph);

                CompressResult result = await CompressAsync(ph, ms, compressionMethod, compressionLevel, cancellationToken);
                r = r.WithAddedSizes(result.ColumnSizes);
                using IMemoryOwner<byte> _ = result.PageData;
                using MemoryStream _1 = result.HeaderMs;

                await orderedCommitGate.WaitForCommit(commitToken);

                // from this point on, we are back to writing on the stream
                if(dictWriteState.HasValue) {
                    await WriteAsync(stream, dictWriteState.Value.dictCompressResult);
                }

                await WriteAsync(stream, result);
            }

        } finally { // sadly need to cleanup manually due to the optional lifetime.
            if(dictWriteState.HasValue) {
                (CompressResult dictCompressResult, MemoryStream ms) = dictWriteState.Value;
                ms.Dispose();
                dictCompressResult.PageData.Dispose();
                dictCompressResult.HeaderMs.Dispose();
            }

            orderedCommitGate.CommitDone(commitToken);
        }

        return r;
    }


    private static void WriteLevels(Stream s, Span<int> levels, int count, int maxValue) {
        int bitWidth = maxValue.GetBitWidth();
        RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.Slice(0, count));
    }
}
