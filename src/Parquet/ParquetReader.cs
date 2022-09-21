using Parquet.File;
using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using Parquet.Data.Rows;
using System.Threading.Tasks;
using System.Threading;

namespace Parquet {
    /// <summary>
    /// Implements Apache Parquet format reader, experimental version for next major release.
    /// </summary>
    public class ParquetReader : ParquetActor, IDisposable {
        private readonly Stream _input;
        private Thrift.FileMetaData _meta;
        private ThriftFooter _footer;
        private readonly ParquetOptions _parquetOptions;
        private readonly List<ParquetRowGroupReader> _groupReaders = new List<ParquetRowGroupReader>();
        private readonly bool _leaveStreamOpen;

        private ParquetReader(Stream input, bool leaveStreamOpen) : base(input) {
            _input = input ?? throw new ArgumentNullException(nameof(input));
            _leaveStreamOpen = leaveStreamOpen;
        }

        private ParquetReader(Stream input, ParquetOptions parquetOptions = null, bool leaveStreamOpen = true) : this(input, leaveStreamOpen) {
            if(!input.CanRead || !input.CanSeek)
                throw new ArgumentException("stream must be readable and seekable", nameof(input));
            if(_input.Length <= 8)
                throw new IOException("not a Parquet file (size too small)");

            _parquetOptions = parquetOptions ?? new ParquetOptions();
        }

        private async Task InitialiseAsync(CancellationToken cancellationToken) {
            ValidateFile();

            //read metadata instantly, now
            _meta = await ReadMetadataAsync(cancellationToken);
            _footer = new ThriftFooter(_meta);

            InitRowGroupReaders();
        }

        /// <summary>
        /// Opens reader from a file on disk. When the reader is disposed the file handle is automatically closed.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="parquetOptions"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<ParquetReader> CreateAsync(string filePath,
            ParquetOptions parquetOptions = null,
            CancellationToken cancellationToken = default) {
            Stream fs = System.IO.File.OpenRead(filePath);
            var reader = new ParquetReader(fs, parquetOptions, false);
            await reader.InitialiseAsync(cancellationToken);
            return reader;
        }

        /// <summary>
        /// Creates an instance from input stream
        /// </summary>
        /// <param name="input">Input stream, must be readable and seekable</param>
        /// <param name="parquetOptions">Optional reader options</param>
        /// <param name="leaveStreamOpen">When true, leaves the stream passed in <paramref name="input"/> open after disposing the reader.</param>
        /// <param name="cancellationToken"></param>
        /// <exception cref="ArgumentNullException">input</exception>
        /// <exception cref="ArgumentException">stream must be readable and seekable - input</exception>
        /// <exception cref="IOException">not a Parquet file (size too small)</exception>
        public static async Task<ParquetReader> CreateAsync(
            Stream input, ParquetOptions parquetOptions = null, bool leaveStreamOpen = true,
            CancellationToken cancellationToken = default) {

            var reader = new ParquetReader(input, parquetOptions, leaveStreamOpen);
            await reader.InitialiseAsync(cancellationToken);
            return reader;
        }

        /// <summary>
        /// Gets custom key-value pairs for metadata
        /// </summary>
        public Dictionary<string, string> CustomMetadata => _footer.CustomMetadata;


        #region [ Helpers ]

        /// <summary>
        /// Reads entire file as a table
        /// </summary>
        public static async Task<Table> ReadTableFromFileAsync(string filePath, ParquetOptions parquetOptions = null) {
            using(ParquetReader reader = await CreateAsync(filePath, parquetOptions)) {
                return await reader.ReadAsTableAsync();
            }
        }

        /// <summary>
        /// Reads entire stream as a table
        /// </summary>
        public static async Task<Table> ReadTableFromStreamAsync(Stream stream, ParquetOptions parquetOptions = null) {
            using(ParquetReader reader = await CreateAsync(stream, parquetOptions)) {
                return await reader.ReadAsTableAsync();
            }
        }

        #endregion

        /// <summary>
        /// Gets the number of rows groups in this file
        /// </summary>
        public int RowGroupCount => _meta.Row_groups.Count;

        /// <summary>
        /// Reader schema
        /// </summary>
        public Schema Schema => _footer.CreateModelSchema(_parquetOptions);

        /// <summary>
        /// Internal parquet metadata
        /// </summary>
        public Thrift.FileMetaData ThriftMetadata => _meta;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public ParquetRowGroupReader OpenRowGroupReader(int index) {
            return _groupReaders[index];
        }

        /// <summary>
        /// Reads entire row group's data columns in one go.
        /// </summary>
        /// <param name="rowGroupIndex">Index of the row group. Default to the first row group if not specified.</param>
        /// <returns></returns>
        public async Task<DataColumn[]> ReadEntireRowGroupAsync(int rowGroupIndex = 0) {
            DataField[] dataFields = Schema.GetDataFields();
            DataColumn[] result = new DataColumn[dataFields.Length];

            using(ParquetRowGroupReader reader = OpenRowGroupReader(rowGroupIndex)) {
                for(int i = 0; i < dataFields.Length; i++) {
                    DataColumn column = await reader.ReadColumnAsync(dataFields[i]);
                    result[i] = column;
                }
            }

            return result;
        }

        private void InitRowGroupReaders() {
            _groupReaders.Clear();

            foreach(Thrift.RowGroup thriftRowGroup in _meta.Row_groups) {
                _groupReaders.Add(new ParquetRowGroupReader(thriftRowGroup, _footer, Stream, ThriftStream, _parquetOptions));
            }
        }

        /// <summary>
        /// Disposes 
        /// </summary>
        public void Dispose() {
            if(!_leaveStreamOpen) {
                _input.Dispose();
            }
        }
    }
}