using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.File;
using Parquet.Meta;
using Parquet.Extensions;

namespace Parquet {
    /// <summary>
    /// Implements Apache Parquet format writer
    /// </summary>
    public sealed class ParquetWriter : ParquetActor, IDisposable, IAsyncDisposable {
        private ThriftFooter? _footer;
        private readonly ParquetSchema _schema;
        private readonly ParquetOptions _formatOptions;
        private bool _dataWritten;
        private readonly List<ParquetRowGroupWriter> _openedWriters = new List<ParquetRowGroupWriter>();

        /// <summary>
        /// Type of compression to use, defaults to <see cref="CompressionMethod.Snappy"/>
        /// </summary>
        public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

        /// <summary>
        /// Level of compression
        /// </summary>
#if NET6_0_OR_GREATER
        public CompressionLevel CompressionLevel = CompressionLevel.SmallestSize;
#else
        public CompressionLevel CompressionLevel = CompressionLevel.Optimal;
#endif

        private ParquetWriter(ParquetSchema schema, Stream output, ParquetOptions? formatOptions = null, bool append = false)
           : base(output.CanSeek == true ? output : new MeteredWriteStream(output)) {
            if(output == null)
                throw new ArgumentNullException(nameof(output));

            if(!output.CanWrite)
                throw new ArgumentException("stream is not writeable", nameof(output));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _formatOptions = formatOptions ?? new ParquetOptions();
        }

        /// <summary>
        /// Creates an instance of parquet writer on top of a stream
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="output">Writeable, seekable stream</param>
        /// <param name="formatOptions">Additional options</param>
        /// <param name="append"></param>
        /// <param name="cancellationToken"></param>
        /// <exception cref="ArgumentNullException">Output is null.</exception>
        /// <exception cref="ArgumentException">Output stream is not writeable</exception>
        public static async Task<ParquetWriter> CreateAsync(
            ParquetSchema schema, Stream output, ParquetOptions? formatOptions = null, bool append = false,
            CancellationToken cancellationToken = default) {
            var writer = new ParquetWriter(schema, output, formatOptions, append);
            await writer.PrepareFileAsync(append, cancellationToken);
            return writer;
        }

        /// <summary>
        /// Creates a new row group and a writer for it.
        /// </summary>
        public ParquetRowGroupWriter CreateRowGroup() {
            _dataWritten = true;

            var writer = new ParquetRowGroupWriter(_schema, Stream, _footer!,
               CompressionMethod, _formatOptions, CompressionLevel);

            _openedWriters.Add(writer);

            return writer;
        }

        /// <summary>
        /// Gets custom key-value pairs for metadata
        /// </summary>
        public IReadOnlyDictionary<string, string> CustomMetadata {
            get => _footer!.CustomMetadata;
            set => _footer!.CustomMetadata = value.ToDictionary(p => p.Key, p => p.Value);
        }

        private async Task PrepareFileAsync(bool append, CancellationToken cancellationToken) {
            if(append) {
                if(!Stream.CanSeek)
                    throw new IOException("destination stream must be seekable for append operations.");

                if(Stream.Length == 0)
                    throw new IOException($"you can only append to existing streams, but current stream is empty.");

                await ValidateFileAsync();

                FileMetaData fileMeta = await ReadMetadataAsync(cancellationToken);
                _footer = new ThriftFooter(fileMeta);

                ValidateSchemasCompatible(_footer, _schema);

                await GoBeforeFooterAsync();
            } else {
                if(_footer == null) {
                    _footer = new ThriftFooter(_schema, 0 /* todo: don't forget to set the total row count at the end!!! */);

                    //file starts with magic
                    await WriteMagicAsync();
                } else {
                    ValidateSchemasCompatible(_footer, _schema);

                    _footer.Add(0 /* todo: don't forget to set the total row count at the end!!! */);
                }
            }
        }

        private void ValidateSchemasCompatible(ThriftFooter footer, ParquetSchema schema) {
            ParquetSchema existingSchema = footer.CreateModelSchema(_formatOptions);

            if(!schema.Equals(existingSchema)) {
                string reason = schema.GetNotEqualsMessage(existingSchema, "appending", "existing");
                throw new ParquetException($"passed schema does not match existing file schema, reason: {reason}");
            }
        }

        private void WriteMagic() => Stream.Write(MagicBytes, 0, MagicBytes.Length);
        private Task WriteMagicAsync() => Stream.WriteAsync(MagicBytes, 0, MagicBytes.Length);

        private void DisposeCore() {
            if(_dataWritten) {
                //update row count (on append add row count to existing metadata)
                _footer!.Add(_openedWriters.Sum(w => w.RowCount ?? 0));
            }
        }

        /// <summary>
        /// Disposes the writer and writes the file footer.
        /// </summary>
        public void Dispose() {

            DisposeCore();

            if(_footer == null)
                return;

            long size = _footer.Write(Stream);
            Stream.WriteInt32((int)size); // metadata size: 4 bytes
            WriteMagic();                 // end magic:     4 bytes
            Stream.Flush();
        }

        /// <summary>
        /// Dispose the writer asynchronously
        /// </summary>
        public async ValueTask DisposeAsync() {
            DisposeCore();

            if(_footer == null)
                return;

            long size = await _footer.WriteAsync(Stream).ConfigureAwait(false);
            await Stream.WriteInt32Async((int)size);
            await WriteMagicAsync();
            await Stream.FlushAsync();
        }
    }
}