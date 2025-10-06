using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using IronCompress;
using Microsoft.IO;
using Parquet.Bloom;
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

            ColumnSizes columnSizes = await WriteColumnAsync(
                chunk, column, _schemaElement,
                cancellationToken);
            //generate stats for column chunk
            chunk.MetaData!.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);

            //the following counters must include both data size and header size
            chunk.MetaData.TotalCompressedSize = columnSizes.CompressedSize;
            chunk.MetaData.TotalUncompressedSize = columnSizes.UncompressedSize;

            return chunk;
        }

        class ColumnSizes {
            public int CompressedSize;
            public int UncompressedSize;
        }

        private async Task CompressAndWriteAsync(
            PageHeader ph, MemoryStream data,
            ColumnSizes cs,
            CancellationToken cancellationToken) {
            
            using IronCompress.IronCompressResult compressedData = _compressionMethod == CompressionMethod.None
                ? new IronCompress.IronCompressResult(data.ToArray(), Codec.Snappy, false)
                : Compressor.Compress(_compressionMethod, data.ToArray(), _compressionLevel);
            
            ph.UncompressedPageSize = (int)data.Length;
            ph.CompressedPageSize = compressedData.AsSpan().Length;

            //write the header in
            using MemoryStream headerMs = _rmsMgr.GetStream();
            ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
            int headerSize = (int)headerMs.Length;
            headerMs.Position = 0;
            _stream.Flush();

            await headerMs.CopyToAsync(_stream);

            // write data
            _stream.WriteSpan(compressedData);

            cs.CompressedSize += headerSize;
            cs.UncompressedSize += headerSize;

            cs.CompressedSize += ph.CompressedPageSize;
            cs.UncompressedSize += ph.UncompressedPageSize;
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

            BloomCollector? bloom = null;
            if(_options.BloomFilterOptionsByColumn.TryGetValue(column.Field.Name, out ParquetOptions.BloomFilterOptions? bloomOptions)) {
                if(bloomOptions != null && bloomOptions.EnableBloomFilters) {
                    BloomSizing.BloomPlan plan = BloomSizing.Plan(
                        estimatedDistinctValues: column.Statistics?.DistinctCount ?? column.NumValues,
                        targetFpp: bloomOptions.BloomFilterFpp,
                        bitsPerValueOverride: bloomOptions.BloomFilterBitsPerValueOverride);
                    bloom = new BloomCollector(plan.Blocks);
                }
            }

            // dictionary page
            if(pc.HasDictionary) {
                PageHeader ph = _footer.CreateDictionaryPage(pc.Dictionary!.Length);
                using MemoryStream ms = _rmsMgr.GetStream();
                ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                       tse,
                       ms, column.Statistics);

                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
                if(bloom != null) {
                    BloomAddValues(bloom, pc.Dictionary, 0, pc.Dictionary.Length, _schemaElement);
                }
            }

            // data page
            using(MemoryStream ms = _rmsMgr.GetStream()) {
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
                    if(bloom != null) {
                        BloomAddValues(bloom, data, offset, count, _schemaElement);
                    }
                    if(deltaEncode) {
                        DeltaBinaryPackedEncoder.Encode(data, offset, count, ms, column.Statistics);
                        chunk.MetaData!.Encodings[2] = Encoding.DELTA_BINARY_PACKED;
                    } else {
                        ParquetPlainEncoder.Encode(data, offset, count, tse, ms, pc.HasDictionary ? null : column.Statistics);
                    }
                }

                ph.DataPageHeader!.Statistics = column.Statistics!.ToThriftStatistics(tse);
                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
            }

            if(bloom != null && chunk?.MetaData != null) {
                BloomFilterIO.WriteToStream(
                    _stream,
                    bloom.Filter,
                    chunk.MetaData,
                    s => new Meta.Proto.ThriftCompactProtocolWriter(s));
            }

            return r;
        }

        private static void WriteLevels(Stream s, Span<int> levels, int count, int maxValue) {
            int bitWidth = maxValue.GetBitWidth();
            RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.Slice(0, count));
        }

        private static void BloomAddValues(BloomCollector bloom, Array values, int offset, int count, SchemaElement tse) {
            switch(tse.Type!.Value) {
                case Meta.Type.BOOLEAN: {
                        if(values is bool[] a)
                            for(int i = 0; i < count; i++)
                                bloom.AddBoolean(a[offset + i]);
                        break;
                    }
                case Meta.Type.INT32: {
                        if(values is int[] a)
                            for(int i = 0; i < count; i++)
                                bloom.AddInt32(a[offset + i]);
                        else if(values is uint[] au)
                            for(int i = 0; i < count; i++)
                                bloom.AddInt32(unchecked((int)au[offset + i]));
                        break;
                    }
                case Meta.Type.INT64: {
                        if(values is long[] a)
                            for(int i = 0; i < count; i++)
                                bloom.AddInt64(a[offset + i]);
                        else if(values is ulong[] au)
                            for(int i = 0; i < count; i++)
                                bloom.AddInt64(unchecked((long)au[offset + i]));
                        break;
                    }
                case Meta.Type.INT96: {
                        if(values is DateTime[] a)
                            for(int i = 0; i < count; i++)
                                bloom.AddInt96(a[offset + i]);
                        break;
                    }
                case Meta.Type.FLOAT: {
                        if(values is float[] a)
                            for(int i = 0; i < count; i++)
                                bloom.AddFloat(a[offset + i]);
                        break;
                    }
                case Meta.Type.DOUBLE: {
                        if(values is double[] a)
                            for(int i = 0; i < count; i++)
                                bloom.AddDouble(a[offset + i]);
                        break;
                    }
                case Meta.Type.BYTE_ARRAY: {
                        if(values is string[] sa) {
                            for(int i = 0; i < count; i++)
                                bloom.AddString(sa[offset + i]);
                        } else if(values is byte[][] ba) {
                            for(int i = 0; i < count; i++)
                                bloom.AddByteArray(ba[offset + i]);
                        } else if(values is Array any && any.Length > 0 && any.GetValue(0) is byte[]) {
                            // Handles jagged byte[][] typed as Array
                            for(int i = 0; i < count; i++)
                                bloom.AddByteArray((byte[])any.GetValue(offset + i)!);
                        }
                        break;
                    }
                case Meta.Type.FIXED_LEN_BYTE_ARRAY: {
                        if(values is byte[][] ba) {
                            for(int i = 0; i < count; i++)
                                bloom.AddFixed(ba[offset + i]);
                        } else if(values is Array any && any.Length > 0 && any.GetValue(0) is byte[]) {
                            for(int i = 0; i < count; i++)
                                bloom.AddFixed((byte[])any.GetValue(offset + i)!);
                        }
                        break;
                    }
                default:
                    break;
            }
        }
    }
}
