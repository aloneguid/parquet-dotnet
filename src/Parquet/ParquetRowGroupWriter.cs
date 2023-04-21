using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File;
using Parquet.Schema;
using FieldPath = Parquet.Schema.FieldPath;

namespace Parquet {
    /// <summary>
    /// Writer for Parquet row groups
    /// </summary>
#pragma warning disable CA1063 // Implement IDisposable Correctly
    public class ParquetRowGroupWriter : IDisposable
#pragma warning restore CA1063 // Implement IDisposable Correctly
    {
        private readonly ParquetSchema _schema;
        private readonly Stream _stream;
        private readonly ThriftFooter _footer;
        private readonly CompressionMethod _compressionMethod;
        private readonly CompressionLevel _compressionLevel;
        private readonly ParquetOptions _formatOptions;
        private readonly Thrift.RowGroup _thriftRowGroup;
        private readonly Thrift.SchemaElement[] _thschema;
        private int _colIdx;

        internal ParquetRowGroupWriter(ParquetSchema schema,
           Stream stream,
           ThriftFooter footer,
           CompressionMethod compressionMethod,
           ParquetOptions formatOptions,
           CompressionLevel compressionLevel) {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _compressionMethod = compressionMethod;
            _compressionLevel = compressionLevel;
            _formatOptions = formatOptions;

            _thriftRowGroup = _footer.AddRowGroup();
            _thriftRowGroup.Columns = new List<Thrift.ColumnChunk>();
            _thschema = _footer.GetWriteableSchema();
        }

        internal long? RowCount { get; private set; }

        /// <summary>
        /// Writes next data column to parquet stream. Note that columns must be written in the order they are declared in the
        /// file schema.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="cancellationToken"></param>
        public async Task WriteColumnAsync(DataColumn column, CancellationToken cancellationToken = default) {
            if(column == null)
                throw new ArgumentNullException(nameof(column));

            if(RowCount == null) {
                if(column.NumValues > 0 || column.Field.MaxRepetitionLevel == 0)
                    RowCount = column.CalculateRowCount();
            }

            Thrift.SchemaElement tse = _thschema[_colIdx];
            if(!column.Field.Equals(tse)) {
                throw new ArgumentException($"cannot write this column, expected '{tse.Name}', passed: '{column.Field.Name}'", nameof(column));
            }
            _colIdx += 1;

            FieldPath path = _footer.GetPath(tse);

            var writer = new DataColumnWriter(_stream, _footer, tse,
               _compressionMethod,
               _formatOptions,
               _compressionLevel);

            Thrift.ColumnChunk chunk = await writer.WriteAsync(path, column, cancellationToken);
            _thriftRowGroup.Columns.Add(chunk);

        }

        /// <summary>
        /// 
        /// </summary>
#pragma warning disable CA1063 // Implement IDisposable Correctly
        public void Dispose()
#pragma warning restore CA1063 // Implement IDisposable Correctly
        {
            //todo: check if all columns are present

            //row count is know only after at least one column is written
            _thriftRowGroup.Num_rows = RowCount ?? 0;

            //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
            //luckily ColumnChunk already contains sizes of page+header in it's meta
            _thriftRowGroup.Total_byte_size = _thriftRowGroup.Columns.Sum(c => c.Meta_data.Total_compressed_size);
        }
    }
}