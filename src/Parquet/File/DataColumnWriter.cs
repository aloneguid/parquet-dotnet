using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File.Values;
using Parquet.Schema;

namespace Parquet.File {
    class DataColumnWriter {
        private readonly Stream _stream;
        private readonly ThriftStream _thriftStream;
        private readonly ThriftFooter _footer;
        private readonly Thrift.SchemaElement _schemaElement;
        private readonly CompressionMethod _compressionMethod;
        private readonly int _rowCount;
        private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

        private struct PageTag {
            public int HeaderSize;
            public Thrift.PageHeader HeaderMeta;
        }

        public DataColumnWriter(
           Stream stream,
           ThriftStream thriftStream,
           ThriftFooter footer,
           Thrift.SchemaElement schemaElement,
           CompressionMethod compressionMethod,
           int rowCount) {
            _stream = stream;
            _thriftStream = thriftStream;
            _footer = footer;
            _schemaElement = schemaElement;
            _compressionMethod = compressionMethod;
            _rowCount = rowCount;
        }

        public async Task<Thrift.ColumnChunk> WriteAsync(
            FieldPath fullPath, DataColumn column,
            CancellationToken cancellationToken = default) {

            Thrift.ColumnChunk chunk = _footer.CreateColumnChunk(_compressionMethod, _stream, _schemaElement.Type, fullPath, 0);
            Thrift.PageHeader ph = _footer.CreateDataPage(column.Count);
            _footer.GetLevels(chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);

            List<PageTag> pages = await WriteColumnAsync(column, _schemaElement, maxRepetitionLevel, maxDefinitionLevel, cancellationToken);
            //generate stats for column chunk
            chunk.Meta_data.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);

            //this count must be set to number of all values in the column, including nulls.
            //for hierarchy/repeated columns this is a count of flattened list, including nulls.
            chunk.Meta_data.Num_values = ph.Data_page_header.Num_values;
            ph.Data_page_header.Statistics = chunk.Meta_data.Statistics;   //simply copy statistics to page header

            //the following counters must include both data size and header size
            chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
            chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

            return chunk;
        }

        private async Task<List<PageTag>> WriteColumnAsync(DataColumn column,
           Thrift.SchemaElement tse,
           int maxRepetitionLevel,
           int maxDefinitionLevel,
           CancellationToken cancellationToken = default) {

            var pages = new List<PageTag>();

            /*
             * Page header must preceeed actual data (compressed or not) however it contains both
             * the uncompressed and compressed data size which we don't know! This somehow limits
             * the write efficiency.
             */

            // todo: replace with RecycleableMemoryStream: https://github.com/microsoft/Microsoft.IO.RecyclableMemoryStream
            byte[] uncompressedData;
            using(var ms = new MemoryStream()) {

                //chain streams together so we have real streaming instead of wasting undefraggable LOH memory
                if(column.RepetitionLevels != null) {
                    WriteLevels(ms, column.RepetitionLevels, column.RepetitionLevels.Length, maxRepetitionLevel);
                }

                Array data = column.Data;
                int dataOffset = column.Offset;
                int dataCount = column.Count;

                if(maxDefinitionLevel > 0) {
                    int nullCount = column.CalculateNullCount();
                    column.Statistics.NullCount = nullCount;

                    // having exact null count we can allocate/rent just the right buffer
                    Array packedData = column.Field.CreateArray(column.Count - nullCount);
                    int[] definitions = IntPool.Rent(column.Count);
                    try {
                        column.PackDefinitions(definitions.AsSpan(0, column.Count),
                            data, dataOffset, dataCount,
                            packedData,
                            maxDefinitionLevel);

                        WriteLevels(ms, definitions, column.Count, maxDefinitionLevel);

                        // levels extracted and written out, swap data to packed data
                        data = packedData;
                        dataOffset = 0;
                        dataCount = column.Count - nullCount;

                    }
                    finally {
                        IntPool.Return(definitions);
                    }
                }
                else {
                    //no defitions means no nulls
                    column.Statistics.NullCount = 0;
                }

                if(!ParquetEncoder.Encode(data, dataOffset, dataCount,
                    tse,
                    ms, column.Statistics)) {

                    throw new IOException("failed to encode data");
                }
                uncompressedData = ms.ToArray();
            }

            using(IronCompress.DataBuffer compressedData = _compressionMethod == CompressionMethod.None
                ? new IronCompress.DataBuffer(uncompressedData)
                : Compressor.Compress(_compressionMethod, uncompressedData)) {

                Thrift.PageHeader dataPageHeader = _footer.CreateDataPage(column.Count);
                dataPageHeader.Uncompressed_page_size = uncompressedData.Length;
                dataPageHeader.Compressed_page_size = compressedData.AsSpan().Length;

                //write the header in
                dataPageHeader.Data_page_header.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);
                int headerSize = await _thriftStream.WriteAsync(dataPageHeader, false, cancellationToken);
#if NETSTANDARD2_0
                byte[] tmp = compressedData.AsSpan().ToArray();
                _stream.Write(tmp, 0, tmp.Length);
#else
                _stream.Write(compressedData);
#endif

                var dataTag = new PageTag {
                    HeaderMeta = dataPageHeader,
                    HeaderSize = headerSize
                };

                pages.Add(dataTag);
            }

            return pages;
        }

        private void WriteLevels(Stream s, int[] levels, int count, int maxLevel) {
            int bitWidth = maxLevel.GetBitWidth();
            RunLengthBitPackingHybridValuesWriter.WriteForwardOnly(s, bitWidth, levels, count);
        }
    }
}