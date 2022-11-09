using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File.Values;

namespace Parquet.File {
    class DataColumnWriter {
        private readonly Stream _stream;
        private readonly ThriftStream _thriftStream;
        private readonly ThriftFooter _footer;
        private readonly Thrift.SchemaElement _schemaElement;
        private readonly CompressionMethod _compressionMethod;
        private readonly int _rowCount;

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

        public async Task<Thrift.ColumnChunk> WriteAsync(List<string> path, DataColumn column, IDataTypeHandler dataTypeHandler, CancellationToken cancellationToken = default) {
            Thrift.ColumnChunk chunk = _footer.CreateColumnChunk(_compressionMethod, _stream, _schemaElement.Type, path, 0);
            Thrift.PageHeader ph = _footer.CreateDataPage(column.Count);
            _footer.GetLevels(chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);

            List<PageTag> pages = await WriteColumnAsync(column, _schemaElement, dataTypeHandler, maxRepetitionLevel, maxDefinitionLevel, cancellationToken);
            //generate stats for column chunk
            chunk.Meta_data.Statistics = column.Statistics.ToThriftStatistics(dataTypeHandler, _schemaElement);

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
           IDataTypeHandler dataTypeHandler,
           int maxRepetitionLevel,
           int maxDefinitionLevel,
           CancellationToken cancellationToken = default) {

            var pages = new List<PageTag>();

            /*
             * Page header must preceeed actual data (compressed or not) however it contains both
             * the uncompressed and compressed data size which we don't know! This somehow limits
             * the write efficiency.
             */

            byte[] uncompressedData;
            using(var ms = new MemoryStream()) {

                //chain streams together so we have real streaming instead of wasting undefraggable LOH memory
                using(var writer = new BinaryWriter(ms, Encoding.UTF8)) {
                    if(column.RepetitionLevels != null) {
                        WriteLevels(writer, column.RepetitionLevels, column.RepetitionLevels.Length, maxRepetitionLevel);
                    }

                    ArrayView data = new ArrayView(column.Data, column.Offset, column.Count);

                    if(maxDefinitionLevel > 0) {
                        data = column.PackDefinitions(maxDefinitionLevel,
                            out int[] definitionLevels,
                            out int definitionLevelsLength,
                            out int nullCount);

                        //last chance to capture null count as null data is compressed now
                        column.Statistics.NullCount = nullCount;

                        try {
                            WriteLevels(writer, definitionLevels, definitionLevelsLength, maxDefinitionLevel);
                        }
                        finally {
                            if(definitionLevels != null) {
                                ArrayPool<int>.Shared.Return(definitionLevels);
                            }
                        }
                    }
                    else {
                        //no defitions means no nulls
                        column.Statistics.NullCount = 0;
                    }

                    dataTypeHandler.Write(tse, writer, data, column.Statistics);

                    //writer.Flush();
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
                dataPageHeader.Data_page_header.Statistics = column.Statistics.ToThriftStatistics(dataTypeHandler, _schemaElement);
                int headerSize = await _thriftStream.WriteAsync(dataPageHeader, false, cancellationToken);
                _stream.Write(compressedData);

                var dataTag = new PageTag {
                    HeaderMeta = dataPageHeader,
                    HeaderSize = headerSize
                };

                pages.Add(dataTag);
            }

            return pages;
        }

        private void WriteLevels(BinaryWriter writer, int[] levels, int count, int maxLevel) {
            int bitWidth = maxLevel.GetBitWidth();
            RunLengthBitPackingHybridValuesWriter.WriteForwardOnly(writer, bitWidth, levels, count);
        }
    }
}