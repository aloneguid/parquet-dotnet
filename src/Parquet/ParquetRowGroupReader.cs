﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Reader for Parquet row groups
    /// </summary>
    public class ParquetRowGroupReader : IDisposable {
        private readonly Thrift.RowGroup _rowGroup;
        private readonly ThriftFooter _footer;
        private readonly Stream _stream;
        private readonly ThriftStream _thriftStream;
        private readonly ParquetOptions _parquetOptions;
        private readonly Dictionary<FieldPath, Thrift.ColumnChunk> _pathToChunk = new Dictionary<FieldPath, Thrift.ColumnChunk>();

        internal ParquetRowGroupReader(
           Thrift.RowGroup rowGroup,
           ThriftFooter footer,
           Stream stream, ThriftStream thriftStream,
           ParquetOptions parquetOptions) {
            _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _thriftStream = thriftStream ?? throw new ArgumentNullException(nameof(thriftStream));
            _parquetOptions = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

            //cache chunks
            foreach(Thrift.ColumnChunk thriftChunk in _rowGroup.Columns) {
                FieldPath path = thriftChunk.GetPath();
                _pathToChunk[path] = thriftChunk;
            }
        }

        /// <summary>
        /// Gets the number of rows in this row group
        /// </summary>
        public long RowCount => _rowGroup.Num_rows;

        /// <summary>
        /// Reads a column from this row group.
        /// </summary>
        /// <param name="field"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default) {
            if(field == null)
                throw new ArgumentNullException(nameof(field));

            if(!_pathToChunk.TryGetValue(field.Path, out Thrift.ColumnChunk columnChunk)) {
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