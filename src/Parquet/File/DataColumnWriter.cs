using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using IronCompress;
using Microsoft.IO;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Schema;

namespace Parquet.File {
    class DataColumnWriter {
        private readonly Stream _stream;
        private readonly ThriftFooter _footer;
        private readonly Thrift.SchemaElement _schemaElement;
        private readonly CompressionMethod _compressionMethod;
        private readonly CompressionLevel _compressionLevel;
        private readonly ParquetOptions _options;
        private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();

        public DataColumnWriter(
           Stream stream,
           ThriftFooter footer,
           Thrift.SchemaElement schemaElement,
           CompressionMethod compressionMethod,
           ParquetOptions options,
           CompressionLevel compressionLevel) {
            _stream = stream;
            _footer = footer;
            _schemaElement = schemaElement;
            _compressionMethod = compressionMethod;
            _compressionLevel = compressionLevel;
            _options = options;
        }

        public async Task<Thrift.ColumnChunk> WriteAsync(
            FieldPath fullPath, DataColumn column,
            CancellationToken cancellationToken = default) {

            // Num_values in the chunk does include null values - I have validated this by dumping spark-generated file.
            Thrift.ColumnChunk chunk = _footer.CreateColumnChunk(
                _compressionMethod, _stream, _schemaElement.Type, fullPath, column.NumValues);

            ColumnSizes columnSizes = await WriteColumnAsync(column, _schemaElement,
                cancellationToken);
            //generate stats for column chunk
            chunk.Meta_data.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);

            //the following counters must include both data size and header size
            chunk.Meta_data.Total_compressed_size = columnSizes.CompressedSize;
            chunk.Meta_data.Total_uncompressed_size = columnSizes.UncompressedSize;

            return chunk;
        }

        class ColumnSizes {
            public int CompressedSize;
            public int UncompressedSize;
        }

        private async Task CompressAndWriteAsync(
            Thrift.PageHeader ph, MemoryStream data,
            ColumnSizes cs,
            CancellationToken cancellationToken) {
            
            using IronCompress.IronCompressResult compressedData = _compressionMethod == CompressionMethod.None
                ? new IronCompress.IronCompressResult(data.ToArray(), Codec.Snappy, false)
                : Compressor.Compress(_compressionMethod, data.ToArray(), _compressionLevel);
            
            ph.Uncompressed_page_size = (int)data.Length;
            ph.Compressed_page_size = compressedData.AsSpan().Length;

            //write the header in
            int headerSize = await ThriftIO.WriteAsync(_stream, ph, cancellationToken);
#if NETSTANDARD2_0
                byte[] tmp = compressedData.AsSpan().ToArray();
                _stream.Write(tmp, 0, tmp.Length);
#else
            _stream.Write(compressedData);
#endif
            cs.CompressedSize += headerSize;
            cs.UncompressedSize += headerSize;

            cs.CompressedSize += ph.Compressed_page_size;
            cs.UncompressedSize += ph.Uncompressed_page_size;
        }

        private async Task<ColumnSizes> WriteColumnAsync(DataColumn column,
           Thrift.SchemaElement tse,
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
                Thrift.PageHeader ph = _footer.CreateDictionaryPage(pc.Dictionary!.Length);
                using MemoryStream ms = _rmsMgr.GetStream();
                ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                       tse,
                       ms, column.Statistics);

                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
            }

            // data page
            using(MemoryStream ms = _rmsMgr.GetStream()) {
                // data page Num_values also does include NULLs
                Thrift.PageHeader ph = _footer.CreateDataPage(column.NumValues, pc.HasDictionary);
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
                    ParquetPlainEncoder.Encode(data, offset, count, tse, ms, pc.HasDictionary ? null : column.Statistics);
                }

                ph.Data_page_header.Statistics = column.Statistics.ToThriftStatistics(tse);
                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
            }

            return r;
        }

        private static void WriteLevels(Stream s, Span<int> levels, int count, int maxValue) {
            int bitWidth = maxValue.GetBitWidth();
            RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.Slice(0, count));
        }

        private static void WriteRleDictionary(Stream s, Span<int> data, int maxValue) {

            /*
             * Data page format: https://github.com/apache/parquet-format/blob/master/Encodings.md#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8
             * the bit width used to encode the entry ids stored as 1 byte (max bit width = 32), 
             * followed by the values encoded using RLE/Bit packed described above (with the given bit width).
             */

            int bitWidth = maxValue.GetBitWidth();
            RleBitpackedHybridEncoder.Encode(s, data, bitWidth);

        }
    }
}