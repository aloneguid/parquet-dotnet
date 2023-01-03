using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Thrift;

namespace Parquet.File {

    struct ColumnEncodingResult {
        public long TotalCompressedSize;
        public long TotalUncompressedSize;
    }

    class ColumnEncoder<TClrType> {
        private readonly DataField _field;
        private readonly SchemaElement _schemaElement;
        private readonly FieldPath _pathInSchema;
        private readonly ReadOnlyMemory<TClrType> _data;
        private readonly CompressionMethod _compressionMethod;
        private readonly Stream _outputStream;
        private readonly ThriftFooter _thriftFooter;
        private readonly ThriftStream _thriftStream;

        public ColumnEncoder(
            DataField field,
            Thrift.SchemaElement schemaElement,
            FieldPath pathInSchema,
            ReadOnlyMemory<TClrType> data,
            CompressionMethod compressionMethod,
            Stream outputStream,
            ThriftFooter thriftFooter,
            ThriftStream thriftStream) {

            _field = field;
            _schemaElement = schemaElement;
            _pathInSchema = pathInSchema;
            _data = data;
            _compressionMethod = compressionMethod;
            _outputStream = outputStream;
            _thriftFooter = thriftFooter;
            _thriftStream = thriftStream;
        }

        private Thrift.ColumnChunk CreateColumnChunk() {
            var chunk = new Thrift.ColumnChunk();
            long startPos = _outputStream.Position;
            chunk.File_offset = startPos;
            chunk.Meta_data = new Thrift.ColumnMetaData();
            chunk.Meta_data.Num_values = 0; // to be calculated later
            chunk.Meta_data.Type = _schemaElement.Type;
            chunk.Meta_data.Codec = (Thrift.CompressionCodec)(int)_compressionMethod;
            chunk.Meta_data.Data_page_offset = startPos;
            chunk.Meta_data.Encodings = new List<Thrift.Encoding> {
                Thrift.Encoding.RLE,
                Thrift.Encoding.BIT_PACKED,
                Thrift.Encoding.PLAIN
            };
            chunk.Meta_data.Path_in_schema = _pathInSchema.ToList();
            chunk.Meta_data.Statistics = new Thrift.Statistics();

            return chunk;
        }

        private Thrift.PageHeader CreateDataPage() {
            var ph = new Thrift.PageHeader(Thrift.PageType.DATA_PAGE, 0, 0);
            ph.Data_page_header = new Thrift.DataPageHeader {
                Encoding = Thrift.Encoding.PLAIN,
                Definition_level_encoding = Thrift.Encoding.RLE,
                Repetition_level_encoding = Thrift.Encoding.RLE,
                Num_values = _data.Length,
                Statistics = new Thrift.Statistics()
            };

            return ph;
        }

        public async Task<Thrift.ColumnChunk> Write(CancellationToken cancellationToken = default) {

            Thrift.ColumnChunk chunk = CreateColumnChunk(); // Num_values not set
            Thrift.PageHeader ph = CreateDataPage();

            var result = new ColumnEncodingResult();

            /*
             * Page header must preceeed actual data (compressed or not) however it contains both
             * the uncompressed and compressed data size which we don't know! This somehow limits
             * the write efficiency.
             */

            byte[] uncompressed;
            using(var ms = new MemoryStream()) {    // uncompressed data stream
                
                // todo: write repetition levels

                // todo: write definition levels

                // write actual useful data
                //ReadOnlyMemory<TClrType> data = _data;
                //ReadOnlyMemory<int> t = Unsafe.As<ReadOnlyMemory<TClrType>, ReadOnlyMemory<int>>(ref data);
                //ParquetEncoder.Encode(t.Span, ms);

                uncompressed = ms.ToArray();
            }

            // compress
            using(IronCompress.DataBuffer compressedData = _compressionMethod == CompressionMethod.None
                ? new IronCompress.DataBuffer(uncompressed)
                : Compressor.Compress(_compressionMethod, uncompressed)) {

                Thrift.PageHeader dataPageHeader = _thriftFooter.CreateDataPage(_data.Length);
                dataPageHeader.Uncompressed_page_size = uncompressed.Length;
                dataPageHeader.Compressed_page_size = compressedData.AsSpan().Length;

                //write the header in
                //dataPageHeader.Data_page_header.Statistics = column.Statistics.ToThriftStatistics(dataTypeHandler, _schemaElement);
                int headerSize = await _thriftStream.WriteAsync(dataPageHeader, false, cancellationToken);
#if NETSTANDARD2_0
                byte[] tmp = compressedData.AsSpan().ToArray();
                _outputStream.Write(tmp, 0, tmp.Length);
#else
                _outputStream.Write(compressedData);
#endif

                result.TotalCompressedSize += dataPageHeader.Compressed_page_size + headerSize;
                result.TotalUncompressedSize += dataPageHeader.Uncompressed_page_size + headerSize;
            }

            return chunk;

        }


    }
}
