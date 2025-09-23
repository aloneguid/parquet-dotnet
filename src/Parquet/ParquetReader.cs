using Parquet.File;
using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using System.Threading.Tasks;
using System.Threading;
using Parquet.Schema;
using Parquet.Meta;

namespace Parquet {
    /// <summary>
    /// Implements Apache Parquet format reader, experimental version for next major release.
    /// </summary>
    public class ParquetReader : ParquetActor, IDisposable {
        private readonly Stream _input;
        private FileMetaData? _meta;
        private ThriftFooter _thriftFooter;
        private readonly ParquetOptions _parquetOptions;
        private readonly List<ParquetRowGroupReader> _groupReaders = new();
        private readonly bool _leaveStreamOpen;

        private ParquetReader(Stream input, ParquetOptions? parquetOptions = null, bool leaveStreamOpen = true) : base(input) {
            _input = input ?? throw new ArgumentNullException(nameof(input));
            _leaveStreamOpen = leaveStreamOpen;

            if(!input.CanRead || !input.CanSeek)
                throw new ArgumentException("stream must be readable and seekable", nameof(input));
            if(_input.Length <= 8)
                throw new IOException("not a Parquet file (size too small)");

            _parquetOptions = parquetOptions ?? new ParquetOptions();

            // _fThriftFooter will be initialised right now in the InitialiseAsync
            _thriftFooter = ThriftFooter.Empty;
        }

        private async Task InitialiseAsync(CancellationToken cancellationToken) {
            await ValidateFileAsync();

            //read metadata instantly, now
            _meta = await ReadMetadataAsync(cancellationToken);
            _thriftFooter = new ThriftFooter(_meta);

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
            ParquetOptions? parquetOptions = null,
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
            Stream input, ParquetOptions? parquetOptions = null, bool leaveStreamOpen = true,
            CancellationToken cancellationToken = default) {

            var reader = new ParquetReader(input, parquetOptions, leaveStreamOpen);
            await reader.InitialiseAsync(cancellationToken);
            return reader;
        }

        /// <summary>
        /// Gets custom key-value pairs for metadata
        /// </summary>
        public Dictionary<string, string> CustomMetadata => _thriftFooter.CustomMetadata;


        #region [ Helpers ]

        /// <summary>
        /// Opens file at specified path to read schema and return
        /// </summary>
        public static async Task<ParquetSchema> ReadSchemaAsync(string filePath) {
            using ParquetReader reader = await CreateAsync(filePath);
            return reader.Schema;
        }

        /// <summary>
        /// Reads file stream and returns
        /// </summary>
        public static async Task<ParquetSchema> ReadSchemaAsync(Stream parquetStream) {
            using ParquetReader reader = await CreateAsync(parquetStream);
            return reader.Schema;
        }

        #endregion

        /// <summary>
        /// Gets the number of rows groups in this file
        /// </summary>
        public int RowGroupCount => _meta?.RowGroups.Count ?? -1;

        /// <summary>
        /// Reader schema
        /// </summary>
        public ParquetSchema Schema => _thriftFooter!.CreateModelSchema(_parquetOptions);

        /// <summary>
        /// Internal parquet metadata
        /// </summary>
        public FileMetaData? Metadata => _meta;

        /// <summary>
        /// Opens row group reader. Note that this operation is really cheap as all the metadata is already present.
        /// It only gets expensive when you read actual data, not the metadata itself.
        /// </summary>
        /// <param name="index">Row group index, starting from 0. See <see cref="RowGroupCount"/> to get number of row groups in this file.</param>
        /// <returns></returns>
        public ParquetRowGroupReader OpenRowGroupReader(int index) {
            return _groupReaders[index];
        }

        /// <summary>
        /// Collection of row group readers, fast random access and enumeration
        /// </summary>
        public IReadOnlyList<IParquetRowGroupReader> RowGroups => _groupReaders;

        /// <summary>
        /// Reads entire row group's data columns in one go.
        /// </summary>
        /// <param name="rowGroupIndex">Index of the row group. Default to the first row group if not specified.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns></returns>
        public async Task<DataColumn[]> ReadEntireRowGroupAsync(int rowGroupIndex = 0, CancellationToken cancellationToken = default)
        {
            if(Schema == null)
                throw new InvalidOperationException("schema is not initialised yet");

            DataField[] dataFields = Schema.GetDataFields();
            DataColumn[] result = new DataColumn[dataFields.Length];

            using(ParquetRowGroupReader reader = OpenRowGroupReader(rowGroupIndex))
            {
                for(int i = 0; i < dataFields.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    DataColumn column = await reader.ReadColumnAsync(dataFields[i], cancellationToken);
                    result[i] = column;
                }
            }

            return result;
        }

        private void InitRowGroupReaders() {
            _groupReaders.Clear();
            if(_meta?.RowGroups == null)
                throw new InvalidOperationException("no row groups in metadata");

            foreach(RowGroup rowGroup in _meta.RowGroups) {
                _groupReaders.Add(new ParquetRowGroupReader(rowGroup, _thriftFooter!, Stream, _parquetOptions));
            }
        }

        /// <summary>
        /// <inheritdoc/>
        /// </summary>
        public void Dispose() {
            if(!_leaveStreamOpen) {
                _input.Dispose();
            }
        }
    }
}