using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Reader for Parquet row groups
    /// </summary>
    public class ParquetRowGroupReader : IDisposable {
        private readonly RowGroup _rowGroup;
        private readonly ThriftFooter _footer;
        private readonly Stream _stream;
        private readonly ParquetOptions? _parquetOptions;
        private readonly Dictionary<FieldPath, ColumnChunk> _pathToChunk = new();

        internal ParquetRowGroupReader(
           RowGroup rowGroup,
           ThriftFooter footer,
           Stream stream,
           ParquetOptions? parquetOptions) {
            _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _parquetOptions = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

            //cache chunks
            foreach(ColumnChunk hunk in _rowGroup.Columns) {
                FieldPath path = hunk.GetPath();
                _pathToChunk[path] = hunk;
            }
        }

        /// <summary>
        /// Exposes raw metadata about this row group
        /// </summary>
        public RowGroup owGroup => _rowGroup;

        /// <summary>
        /// Gets the number of rows in this row group
        /// </summary>
        public long RowCount => _rowGroup.NumRows;

        /// <summary>
        /// Reads a column from this row group.
        /// </summary>
        /// <param name="field"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default) {
            if(field == null)
                throw new ArgumentNullException(nameof(field));

            if(!_pathToChunk.TryGetValue(field.Path, out ColumnChunk? columnChunk)) {
                throw new ParquetException($"'{field.Path}' does not exist in this file");
            }

            var columnReader = new DataColumnReader(field, _stream, columnChunk, _footer, _parquetOptions);

            return columnReader.ReadAsync(cancellationToken);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose() {
            //don't need to dispose anything here, but for clarity we implement IDisposable and client must use it as we may add something
            //important in it later
        }

    }
}