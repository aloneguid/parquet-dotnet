using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.File {
    class DataColumnWriter {
        private readonly Stream _stream;
        private readonly ThriftStream _thriftStream;
        private readonly ThriftFooter _footer;
        private readonly Thrift.SchemaElement _schemaElement;
        private readonly CompressionMethod _compressionMethod;
        private readonly CompressionLevel _compressionLevel;
        private readonly ParquetOptions _options;
        private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();

        public DataColumnWriter(
           Stream stream,
           ThriftStream thriftStream,
           ThriftFooter footer,
           Thrift.SchemaElement schemaElement,
           CompressionMethod compressionMethod,
           ParquetOptions options,
           CompressionLevel compressionLevel) {
            _stream = stream;
            _thriftStream = thriftStream;
            _footer = footer;
            _schemaElement = schemaElement;
            _compressionMethod = compressionMethod;
            _compressionLevel = compressionLevel;
            _options = options;
        }

        public async Task<Thrift.ColumnChunk> WriteAsync(
            FieldPath fullPath, DataColumn column,
            CancellationToken cancellationToken = default) {

            Thrift.ColumnChunk chunk = _footer.CreateColumnChunk(
                _compressionMethod, _stream, _schemaElement.Type, fullPath, column.Count);

            _footer.GetLevels(chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);

            ColumnSizes columnSizes = await WriteColumnAsync(column, _schemaElement,
                maxRepetitionLevel, maxDefinitionLevel,
                cancellationToken);
            //generate stats for column chunk
            chunk.Meta_data.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);

            //this count must be set to number of all values in the column, including nulls.
            //for hierarchy/repeated columns this is a count of flattened list, including nulls.
            chunk.Meta_data.Num_values = column.Count;

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
            
            using IronCompress.DataBuffer compressedData = _compressionMethod == CompressionMethod.None
                ? new IronCompress.DataBuffer(data.ToArray())
                : Compressor.Compress(_compressionMethod, data.ToArray(), _compressionLevel);
            
            ph.Uncompressed_page_size = (int)data.Length;
            ph.Compressed_page_size = compressedData.AsSpan().Length;

            //write the header in
            int headerSize = await _thriftStream.WriteAsync(ph, false, cancellationToken);
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
           int maxRepetitionLevel,
           int maxDefinitionLevel,
           CancellationToken cancellationToken = default) {
            var r = new ColumnSizes();

            /*
             * Page header must preceeed actual data (compressed or not) however it contains both
             * the uncompressed and compressed data size which we don't know! This somehow limits
             * the write efficiency.
             */

            using var pc = new PackedColumn(column);
            pc.Pack(maxDefinitionLevel, _options.UseDictionaryEncoding, _options.DictionaryEncodingThreshold);

            // dictionary page
            if(pc.HasDictionary) {
                Thrift.PageHeader ph = _footer.CreateDictionaryPage(pc.Dictionary!.Length);
                using MemoryStream ms = _rmsMgr.GetStream();
                if(!ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                       tse,
                       ms, column.Statistics)) {

                    throw new IOException("failed to encode dictionary data for column " + column);
                }

                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
            }

            // data page
            using(MemoryStream ms = _rmsMgr.GetStream()) {
                Thrift.PageHeader ph = _footer.CreateDataPage(column.Count, pc.HasDictionary);
                if(pc.HasRepetitionLevels) {
                    WriteLevels(ms, pc.RepetitionLevels!, pc.RepetitionLevels!.Length, maxRepetitionLevel);
                }
                if(pc.HasDefinitionLevels) {
                    WriteLevels(ms, pc.DefinitionLevels!, column.Count, maxDefinitionLevel);
                }

                if(pc.HasDictionary) {
                    // dictionary indexes are always encoded with RLE
                    int[] indexes = pc.GetDictionaryIndexes(out int indexesLength)!;
                    int bitWidth = pc.Dictionary!.Length.GetBitWidth();
                    ms.WriteByte((byte)bitWidth);   // bit width is stored as 1 byte before encoded data
                    RleEncoder.Encode(ms, indexes, indexesLength, bitWidth);
                } else {
                    Array data = pc.GetPlainData(out int offset, out int count)!;
                    if(!ParquetPlainEncoder.Encode(data, offset, count, tse, ms, pc.HasDictionary ? null : column.Statistics)) {
                        throw new IOException($"failed to encode data page data for column {column}");
                    }
                }

                ph.Data_page_header.Statistics = column.Statistics.ToThriftStatistics(tse);
                await CompressAndWriteAsync(ph, ms, r, cancellationToken);
            }

            return r;
        }

        private static void WriteLevels(Stream s, int[] levels, int count, int maxValue) {
            int bitWidth = maxValue.GetBitWidth();
            RleEncoder.EncodeWithLength(s, bitWidth, levels, count);
        }

        private static void WriteRleDictionary(Stream s, int[] data, int count, int maxValue) {

            /*
             * Data page format: https://github.com/apache/parquet-format/blob/master/Encodings.md#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8
             * the bit width used to encode the entry ids stored as 1 byte (max bit width = 32), 
             * followed by the values encoded using RLE/Bit packed described above (with the given bit width).
             */

            int bitWidth = maxValue.GetBitWidth();
            RleEncoder.Encode(s, data, count, bitWidth);

        }
    }
}