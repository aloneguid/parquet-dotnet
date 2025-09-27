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
using Parquet.Encryption;

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

        EncryptionBase? _encrypter;
        Meta.FileCryptoMetaData? _cryptoMeta;

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

                FileMetaData fileMeta = await ReadMetadataAsync(cancellationToken: cancellationToken);
                _footer = new ThriftFooter(fileMeta);

                ValidateSchemasCompatible(_footer, _schema);

                await GoBeforeFooterAsync();
            } else {
                if(!string.IsNullOrWhiteSpace(_formatOptions.EncryptionKey)) {
                    byte[]? aadPrefixBytes = _formatOptions.AADPrefix is null
                        ? null
                        : System.Text.Encoding.ASCII.GetBytes(_formatOptions.AADPrefix);

                    if(_formatOptions.SupplyAadPrefix && aadPrefixBytes is null)
                        throw new ArgumentException("SupplyAadPrefix=true requires AADPrefix to be set.");

                    (_encrypter, _cryptoMeta) = EncryptionBase.CreateEncrypterForWrite(
                        _formatOptions.EncryptionKey!,
                        aadPrefixBytes,
                        supplyAadPrefix: _formatOptions.SupplyAadPrefix,
                        useCtrVariant: _formatOptions.UseCtrVariant
                    );

                    this._encrypter = _encrypter ?? throw new InvalidOperationException("encrypter was not created");
                }
                if(_footer == null) {
                    _footer = new ThriftFooter(_schema, 0 /* todo: don't forget to set the total row count at the end!!! */);
                    _footer.Encrypter = _encrypter;
                    //file starts with magic
                    await WriteMagicAsync(encrypted: _encrypter != null);
                } else {
                    ValidateSchemasCompatible(_footer, _schema);
                    _footer.Add(0 /* todo: don't forget to set the total row count at the end!!! */);
                }
                // if(_cryptoMeta is not null) {
                //     var w = new Meta.Proto.ThriftCompactProtocolWriter(Stream);
                //     _cryptoMeta.Write(w);
                // }
            }
        }

        private void ValidateSchemasCompatible(ThriftFooter footer, ParquetSchema schema) {
            ParquetSchema existingSchema = footer.CreateModelSchema(_formatOptions);

            if(!schema.Equals(existingSchema)) {
                string reason = schema.GetNotEqualsMessage(existingSchema, "appending", "existing");
                throw new ParquetException($"passed schema does not match existing file schema, reason: {reason}");
            }
        }

        private void WriteMagic(bool encrypted) => Stream.Write(encrypted ? MagicBytesEncrypted : MagicBytes, 0, MagicBytes.Length);
        private Task WriteMagicAsync(bool encrypted) => Stream.WriteAsync(encrypted ? MagicBytesEncrypted : MagicBytes, 0, MagicBytes.Length);

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

            if(_encrypter is null) {
                long size = _footer.Write(Stream);
                Stream.WriteInt32((int)size);
                WriteMagic(false);
                Stream.Flush();
                return;
            }

            // 1) Serialize plaintext footer to a buffer
            using var ms = new MemoryStream();
            _footer.Write(ms);
            byte[] encFooter = _encrypter.EncryptFooter(ms.ToArray());  // framed: len|nonce|ct|tag

            // 2) Serialize FileCryptoMetaData now (TAIL, not head)
            using var metaMs = new MemoryStream();
            var metaWriter = new Parquet.Meta.Proto.ThriftCompactProtocolWriter(metaMs);
            _cryptoMeta!.Write(metaWriter);
            byte[] metaBytes = metaMs.ToArray();

            // 3) Write [meta][encFooter][combinedLen][ "PARE" ]
            Stream.Write(metaBytes, 0, metaBytes.Length);
            Stream.Write(encFooter, 0, encFooter.Length);
            Stream.WriteInt32(metaBytes.Length + encFooter.Length);
            WriteMagic(true);
            Stream.Flush();
        }


        /// <summary>
        /// Dispose the writer asynchronously
        /// </summary>
        public async ValueTask DisposeAsync() {
            DisposeCore();
            if(_footer == null)
                return;

            if(_encrypter is null) {
                long size = await _footer.WriteAsync(Stream).ConfigureAwait(false);
                await Stream.WriteInt32Async((int)size);
                await WriteMagicAsync(false);
                await Stream.FlushAsync();
                return;
            }

            using var ms = new MemoryStream();
            await _footer.WriteAsync(ms).ConfigureAwait(false);
            byte[] encFooter = _encrypter.EncryptFooter(ms.ToArray());

            using var metaMs = new MemoryStream();
            var metaWriter = new Parquet.Meta.Proto.ThriftCompactProtocolWriter(metaMs);
            _cryptoMeta!.Write(metaWriter);
            byte[] metaBytes = metaMs.ToArray();

            await Stream.WriteAsync(metaBytes, 0, metaBytes.Length);
            await Stream.WriteAsync(encFooter, 0, encFooter.Length);
            await Stream.WriteInt32Async(metaBytes.Length + encFooter.Length);
            await WriteMagicAsync(true);
            await Stream.FlushAsync();
        }
    }
}