using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Encryption;
using Parquet.File;
using Parquet.Meta;
using Parquet.Meta.Proto;
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
        private readonly List<(short colOrd, ColumnChunk cc)> _encryptedColumnChunks = new();

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
            for(int colIdx = 0; colIdx < rowGroup.Columns.Count; colIdx++) {
                ColumnChunk cc = rowGroup.Columns[colIdx];

                // Case 1: Plain metadata present -> register immediately
                if(cc.MetaData != null) {
                    FieldPath path = _footer.GetPath(cc);
                    _pathToChunk[path] = cc;
                    continue;
                }

                // Case 2: Metadata is encrypted -> DO NOT fail here.
                // We may still be able to read plaintext columns in this file without column keys.
                if(cc.EncryptedColumnMetadata != null) {
                    _encryptedColumnChunks ??= new List<(short colOrd, ColumnChunk cc)>();
                    short colOrd = (short)colIdx;
                    _encryptedColumnChunks.Add((colOrd, cc));
                    // We’ll resolve (decrypt or throw) lazily if the caller asks for this column.
                    continue;
                }
                // Truly malformed chunk (neither MetaData nor EncryptedColumnMetadata)
                throw new InvalidDataException("ColumnChunk is missing both MetaData and EncryptedColumnMetadata.");
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
        public bool ColumnExists(DataField field) => GetMetadata(field) != null;


        /// <summary>
        /// Reads a column from this row group. Unlike writing, columns can be read in any order.
        /// If the column is missing, an exception will be thrown.
        /// </summary>
        public Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default) {
            ColumnChunk columnChunk = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            var columnReader = new DataColumnReader(field, _stream,
                columnChunk, ReadColumnStatistics(columnChunk), _footer, _options, _rowGroup);
            return columnReader.ReadAsync(cancellationToken);
        }

        /// <summary>
        /// Gets raw column chunk metadata for this field
        /// </summary>
        public ColumnChunk? GetMetadata(DataField field) {
            if(field == null)
                throw new ArgumentNullException(nameof(field));
            return ResolveChunkFor(field.Path);
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
            Statistics? st = cc.MetaData?.Statistics;

            if((st == null || (st.MinValue == null && st.MaxValue == null && st.NullCount == null)) &&
                cc.EncryptedColumnMetadata != null &&
                cc.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY != null) {
                // Find path and column ordinal
                FieldPath path = _footer.GetPath(cc);
                short rgOrd = _footer.GetRowGroupOrdinal(_rowGroup);
                short colOrd = (short)_rowGroup.Columns.IndexOf(cc);

                // Resolve (will decrypt and cache MetaData)
                ColumnChunk? resolved = ResolveChunkFor(path);
                if(resolved?.MetaData?.Statistics != null)
                    st = resolved.MetaData.Statistics;
            }

            if(st == null)
                return null;

            SchemaElement? se = _footer.GetSchemaElement(cc)
                ?? throw new ArgumentException("can't find schema element", nameof(cc));

            ParquetPlainEncoder.TryDecode(st.MinValue, se, _options!, out object? min);
            ParquetPlainEncoder.TryDecode(st.MaxValue, se, _options!, out object? max);

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

        private ColumnChunk? ResolveChunkFor(FieldPath path) {
            if(_pathToChunk.TryGetValue(path, out ColumnChunk? ccPlain))
                return ccPlain;

            // Try find a matching encrypted chunk by path; if found, decrypt and cache
            for(int i = 0; i < _encryptedColumnChunks.Count; i++) {
                (short colOrd, ColumnChunk? encCc) = _encryptedColumnChunks[i];
                FieldPath encPath = _footer.GetPath(encCc);
                if(!encPath.Equals(path))
                    continue;

                DecryptColumnMetaFor(encCc, encPath, colOrd);
                return encCc; // MetaData now populated & cached
            }

            return null;
        }

        private void DecryptColumnMetaFor(ColumnChunk encCc, FieldPath path, short colOrd) {
            // Column-key info is required on encrypted column meta
            EncryptionWithColumnKey ck = encCc.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY
                     ?? throw new NotSupportedException(
                         $"Column '{path}' has encrypted metadata but lacks column-key crypto metadata.");

            string? keyString = _options?.ColumnKeyResolver?.Invoke(ck.PathInSchema, ck.KeyMetadata);
            if(string.IsNullOrWhiteSpace(keyString))
                throw new NotSupportedException(
                    $"Column '{path}' uses column-key encryption. Provide a ColumnKeyResolver in ParquetOptions to supply the key.");

            byte[] columnKey = EncryptionBase.ParseKeyString(keyString!);

            // We need AAD context (prefix + fileUnique) from the file
            EncryptionBase? ctx = _footer.Decrypter ?? _footer.Encrypter;
            if(ctx == null)
                throw new NotSupportedException($"Column '{path}' metadata is encrypted and AAD context is unavailable.");

            // Create a fresh decrypter with the same algorithm, copy AAD, set the column key
            EncryptionBase dec = ctx is AES_GCM_CTR_V1_Encryption
                ? new AES_GCM_CTR_V1_Encryption()
                : new AES_GCM_V1_Encryption();

            dec.AadPrefix = ctx.AadPrefix;
            dec.AadFileUnique = ctx.AadFileUnique;
            dec.FooterEncryptionKey = columnKey;

            // Decrypt ColumnMetaData (needs row-group & column ordinals)
            using var msEnc = new MemoryStream(encCc.EncryptedColumnMetadata!);
            var tpr = new Parquet.Meta.Proto.ThriftCompactProtocolReader(msEnc);

            short rgOrd = _footer.GetRowGroupOrdinal(_rowGroup);     // <-- use your ThriftFooter helper
            byte[] plain = dec.DecryptColumnMetaData(tpr, rgOrd, colOrd);

            using var ms = new MemoryStream(plain);
            var r = new Parquet.Meta.Proto.ThriftCompactProtocolReader(ms);
            ColumnMetaData realMeta = Meta.ColumnMetaData.Read(r);

            encCc.MetaData = realMeta;            // cache the real meta
            _pathToChunk[path] = encCc;         // make lookups fast next time
        }

        /// <summary>
        /// Dispose isn't required, retained for backward compatibility
        /// </summary>
        public void Dispose() { }
    }
}