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
using System.Security.Cryptography;

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
        private EncryptionBase? _encrypter;
        private Meta.FileCryptoMetaData? _cryptoMeta;

        // for plaintext-footer mode
        private Meta.EncryptionAlgorithm? _plaintextAlg;

        // holds AadPrefix/AadFileUnique to build AAD for signing
        private EncryptionBase? _signer;

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

                FileMetaData fileMeta = await ReadMetadataAsync();
                _footer = new ThriftFooter(fileMeta);

                ValidateSchemasCompatible(_footer, _schema);

                await GoBeforeFooterAsync();
            } else {
                if(!string.IsNullOrWhiteSpace(_formatOptions.FooterEncryptionKey) && !_formatOptions.UsePlaintextFooter) {
                    byte[]? aadPrefixBytes = _formatOptions.AADPrefix is null
                        ? null
                        : System.Text.Encoding.ASCII.GetBytes(_formatOptions.AADPrefix);

                    if(_formatOptions.SupplyAadPrefix && aadPrefixBytes is null)
                        throw new ArgumentException("SupplyAadPrefix=true requires AADPrefix to be set.");

                    (_encrypter, _cryptoMeta) = EncryptionBase.CreateEncryptorForWrite(
                        _formatOptions.FooterEncryptionKey!,
                        aadPrefixBytes,
                        supplyAadPrefix: _formatOptions.SupplyAadPrefix,
                        useCtrVariant: _formatOptions.UseCtrVariant
                    );

                    this._encrypter = _encrypter ?? throw new InvalidOperationException("encrypter was not created");
                }
                if(_footer == null) {
                    _footer = new ThriftFooter(_schema, 0);
                    _footer.Encrypter = _encrypter;
                    // Head magic (PAR1 for plaintext mode, PARE for encrypted footer)
                    bool encryptedFooterMode = _encrypter != null && !_formatOptions.UsePlaintextFooter;
                    await WriteMagicAsync(encrypted: encryptedFooterMode);
                } else {
                    ValidateSchemasCompatible(_footer, _schema);
                    _footer.Add(0);
                }
                // Plaintext footer mode setup: advertise algorithm in FileMetaData and remember signer
                if(_formatOptions.UsePlaintextFooter && !string.IsNullOrWhiteSpace(_formatOptions.FooterSigningKey)) {
                    byte[]? aadPrefixBytes = _formatOptions.AADPrefix is null ? null : System.Text.Encoding.ASCII.GetBytes(_formatOptions.AADPrefix);
                    (EncryptionBase? encTmp, FileCryptoMetaData? cryptoMeta) = EncryptionBase.CreateEncryptorForWrite(
                        _formatOptions.FooterSigningKey!,
                        aadPrefixBytes,
                        supplyAadPrefix: _formatOptions.SupplyAadPrefix,
                        useCtrVariant: _formatOptions.UseCtrVariant
                    );
                    _plaintextAlg = cryptoMeta.EncryptionAlgorithm;
                    _signer = encTmp;             // keep for AAD building during signing
                    _footer.SetPlaintextFooterAlgorithm(_plaintextAlg);
                    // If you have metadata for the signing key, set it here:
                    // _footer.SetFooterSigningKeyMetadata(...);
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
            if(_footer == null) {
                return;
            }

            using var ms = new MemoryStream();

            // --- Plaintext footer mode (always ends with PAR1) ---
            if(_formatOptions.UsePlaintextFooter) {
                _footer.Write(ms);
                byte[] footerBytes = ms.ToArray();

                if(_plaintextAlg is not null) {
                    // Signed plaintext footer (§5.5)
                    if(_signer is null)
                        throw new InvalidOperationException("Signer missing in plaintext footer mode.");

                    byte[] aad = _signer.BuildAad(Meta.ParquetModules.Footer);

                    byte[] nonce12 = new byte[12];
                    CryptoHelpers.FillNonce12(nonce12);

                    byte[] tag = new byte[16];
                    byte[] tmpCt = new byte[footerBytes.Length];

                    // Use the already parsed key on the signer
                    CryptoHelpers.GcmEncryptOrThrow(_signer.FooterEncryptionKey!, nonce12, footerBytes, tmpCt, tag, aad);

                    // [footer][nonce|tag][len=footer+28][PAR1]
                    Stream.Write(footerBytes, 0, footerBytes.Length);
                    Stream.Write(nonce12, 0, nonce12.Length);
                    Stream.Write(tag, 0, tag.Length);
                    Stream.WriteInt32(footerBytes.Length + 28);
                    WriteMagic(false);
                    Stream.Flush();
                    return;
                } else {
                    // Legacy plaintext footer (unsigned)
                    Stream.Write(footerBytes, 0, footerBytes.Length);
                    Stream.WriteInt32(footerBytes.Length);
                    WriteMagic(false);
                    Stream.Flush();
                    return;
                }
            }

            // --- Encrypted footer mode (PARE) ---
            if(_encrypter is not null) {
                _footer.Write(ms);
                byte[] encFooter = _encrypter.EncryptFooter(ms.ToArray());  // framed len|nonce|ct|tag

                using var metaMs = new MemoryStream();
                var metaWriter = new Parquet.Meta.Proto.ThriftCompactProtocolWriter(metaMs);
                _cryptoMeta!.Write(metaWriter);
                byte[] metaBytes = metaMs.ToArray();

                // [meta][encFooter][combinedLen][PARE]
                Stream.Write(metaBytes, 0, metaBytes.Length);
                Stream.Write(encFooter, 0, encFooter.Length);
                Stream.WriteInt32(metaBytes.Length + encFooter.Length);
                WriteMagic(true);
                Stream.Flush();
                return;
            }

            // --- Legacy plaintext footer (no encryption anywhere) ---
            _footer.Write(ms);
            byte[] footerPlain = ms.ToArray();
            Stream.Write(footerPlain, 0, footerPlain.Length);
            Stream.WriteInt32(footerPlain.Length);
            WriteMagic(false);
            Stream.Flush();
        }


        /// <summary>
        /// Dispose the writer asynchronously
        /// </summary>
        public async ValueTask DisposeAsync() {
            DisposeCore();
            if(_footer == null) {
                return;
            }

            using var ms = new MemoryStream();

            // --- Plaintext footer mode (always ends with PAR1) ---
            if(_formatOptions.UsePlaintextFooter) {
                await _footer.WriteAsync(ms).ConfigureAwait(false);
                byte[] footerBytes = ms.ToArray();

                if(_plaintextAlg is not null) {
                    // Signed plaintext footer (§5.5)
                    if(_signer is null)
                        throw new InvalidOperationException("Signer missing in plaintext footer mode.");

                    byte[] aad = _signer.BuildAad(Meta.ParquetModules.Footer);

                    byte[] nonce12 = new byte[12];
                    CryptoHelpers.FillNonce12(nonce12);

                    byte[] tag = new byte[16];
                    byte[] tmpCt = new byte[footerBytes.Length];

                    CryptoHelpers.GcmEncryptOrThrow(_signer.FooterEncryptionKey!, nonce12, footerBytes, tmpCt, tag, aad);

                    // [footer][nonce|tag][len=footer+28][PAR1]
                    await Stream.WriteAsync(footerBytes, 0, footerBytes.Length).ConfigureAwait(false);
                    await Stream.WriteAsync(nonce12, 0, nonce12.Length).ConfigureAwait(false);
                    await Stream.WriteAsync(tag, 0, tag.Length).ConfigureAwait(false);
                    await Stream.WriteInt32Async(footerBytes.Length + 28).ConfigureAwait(false);
                    await WriteMagicAsync(false).ConfigureAwait(false);
                    await Stream.FlushAsync().ConfigureAwait(false);
                    return;
                } else {
                    // Legacy plaintext footer (unsigned)
                    await Stream.WriteAsync(footerBytes, 0, footerBytes.Length).ConfigureAwait(false);
                    await Stream.WriteInt32Async(footerBytes.Length).ConfigureAwait(false);
                    await WriteMagicAsync(false).ConfigureAwait(false);
                    await Stream.FlushAsync().ConfigureAwait(false);
                    return;
                }
            }

            // --- Encrypted footer mode (PARE) ---
            if(_encrypter is not null) {
                ms.SetLength(0);
                await _footer.WriteAsync(ms).ConfigureAwait(false);
                byte[] encFooter = _encrypter.EncryptFooter(ms.ToArray()); // framed len|nonce|ct|tag

                using var metaMs = new MemoryStream();
                var metaWriter = new Parquet.Meta.Proto.ThriftCompactProtocolWriter(metaMs);
                _cryptoMeta!.Write(metaWriter);
                byte[] metaBytes = metaMs.ToArray();

                // [meta][encFooter][combinedLen][PARE]
                await Stream.WriteAsync(metaBytes, 0, metaBytes.Length).ConfigureAwait(false);
                await Stream.WriteAsync(encFooter, 0, encFooter.Length).ConfigureAwait(false);
                await Stream.WriteInt32Async(metaBytes.Length + encFooter.Length).ConfigureAwait(false);
                await WriteMagicAsync(true).ConfigureAwait(false);
                await Stream.FlushAsync().ConfigureAwait(false);
                return;
            }

            // --- Legacy plaintext footer (no encryption anywhere) ---
            ms.SetLength(0);
            await _footer.WriteAsync(ms).ConfigureAwait(false);
            byte[] footerPlain = ms.ToArray();
            await Stream.WriteAsync(footerPlain, 0, footerPlain.Length).ConfigureAwait(false);
            await Stream.WriteInt32Async(footerPlain.Length).ConfigureAwait(false);
            await WriteMagicAsync(false).ConfigureAwait(false);
            await Stream.FlushAsync().ConfigureAwait(false);
        }
    }
}
