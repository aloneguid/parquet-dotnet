using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Operations available on a row group reader, omitting Dispose, which is 
    /// exposed on the implementing class for backward compatibility only.
    /// </summary>
    public interface IParquetRowGroupReader {
        /// <summary>
        /// Exposes raw metadata about this row group
        /// </summary>
        RowGroup RowGroup { get; }

        /// <summary>
        /// Gets the number of rows in this row group
        /// </summary>
        long RowCount { get; }

        /// <summary>
        /// Checks if this field exists in source schema
        /// </summary>
        bool ColumnExists(DataField field);

        /// <summary>
        /// Reads a column from this row group. Unlike writing, columns can be read in any order.
        /// If the column is missing, an exception will be thrown.
        /// </summary>
        Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets raw column chunk metadata for this field
        /// </summary>
        ColumnChunk? GetMetadata(DataField field);

        /// <summary>
        /// Get custom key-value metadata for a data field
        /// </summary>
        Dictionary<string, string> GetCustomMetadata(DataField field);

        /// <summary>
        /// Returns data column statistics for a particular data field
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <exception cref="ParquetException"></exception>
        DataColumnStatistics? GetStatistics(DataField field);
    }

    /// <summary>
    /// Reader for Parquet row groups
    /// </summary>
    public class ParquetRowGroupReader : IDisposable, IParquetRowGroupReader {
        private readonly RowGroup _rowGroup;
        private readonly ThriftFooter _footer;
        private readonly Stream _stream;
        private readonly ParquetOptions? _options;
        private readonly Dictionary<FieldPath, ColumnChunk> _pathToChunk = new();

        internal ParquetRowGroupReader(
           RowGroup rowGroup,
           ThriftFooter footer,
           Stream stream,
           ParquetOptions? parquetOptions) {
            _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _options = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

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
        /// Checks if this field exists in source schema
        /// </summary>
        public bool ColumnExists(DataField field) {
            return GetMetadata(field) != null;
        }

        /// <summary>
        /// Reads a column from this row group. Unlike writing, columns can be read in any order.
        /// If the column is missing, an exception will be thrown.
        /// </summary>
        public Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default) {
            ColumnChunk columnChunk = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            var columnReader = new DataColumnReader(field, _stream,
                columnChunk, ReadColumnStatistics(columnChunk), _footer, _options);
            return columnReader.ReadAsync(cancellationToken);
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

        private DataColumnStatistics? ReadColumnStatistics(ColumnChunk cc) {

            Statistics? st = cc.MetaData!.Statistics;
            if(st == null) return null;

            SchemaElement? se = _footer.GetSchemaElement(cc) ?? throw new ArgumentException("can't find schema element", nameof(cc));

            ParquetPlainEncoder.TryDecode(st.MinValue, se, _options, out object? min);
            ParquetPlainEncoder.TryDecode(st.MaxValue, se, _options, out object? max);

            return new DataColumnStatistics(st.NullCount, st.DistinctCount, min, max);
        }


        /// <summary>
        /// Returns data column statistics for a particular data field
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <exception cref="ParquetException"></exception>
        public DataColumnStatistics? GetStatistics(DataField field) {
            ColumnChunk cc = GetMetadata(field) ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            return ReadColumnStatistics(cc);
        }

        /// <summary>
        /// Dispose isn't required, retained for backward compatibility
        /// </summary>
        public void Dispose() { }
    }
}