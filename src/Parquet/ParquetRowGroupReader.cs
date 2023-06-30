using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        public RowGroup RowGroup => _rowGroup;

        /// <summary>
        /// Gets the number of rows in this row group
        /// </summary>
        public long RowCount => _rowGroup.NumRows;

        /// <summary>
        /// Reads a column from this row group. Unlike writing, columns can be read in any order.
        /// If the column is missing, an exception will be thrown.
        /// </summary>
        public Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default) {

            ColumnChunk? columnChunk = GetMetadata(field);
            if(columnChunk == null) {
                throw new ParquetException($"'{field.Path}' does not exist in this file");
            }
            var columnReader = new DataColumnReader(field, _stream, columnChunk, _footer, _parquetOptions);

            return columnReader.ReadAsync(cancellationToken);
        }

        /// <summary>
        /// Reads a column from this row group. Unlike writing, columns can be read in any order.
        /// If the column is missing, null will be returned.
        /// </summary>
        public async Task<DataColumn?> ReadPotentiallyMissingColumnAsync(DataField field, CancellationToken cancellationToken = default) {

            ColumnChunk? columnChunk = GetMetadata(field);
            if(columnChunk == null) {
                return null;
            }
            var columnReader = new DataColumnReader(field, _stream, columnChunk, _footer, _parquetOptions);

            return await columnReader.ReadAsync(cancellationToken);
        }

        /// <summary>
        /// Gets raw column chunk metadata for this field
        /// </summary>
        public ColumnChunk? GetMetadata(DataField field) {
            if(field == null)
                throw new ArgumentNullException(nameof(field));

            if(!_pathToChunk.TryGetValue(field.Path, out ColumnChunk? columnChunk)) {
                return null;
            }

            return columnChunk;
        }

        /// <summary>
        /// Get custom key-value metadata for a data field
        /// </summary>
        public Dictionary<string, string> GetCustomMetadata(DataField field) {
            ColumnChunk? cc = GetMetadata(field);
            if(cc?.MetaData?.KeyValueMetadata == null)
                return new();

            return cc.MetaData.KeyValueMetadata.ToDictionary(kv => kv.Key, kv => kv.Value!);
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