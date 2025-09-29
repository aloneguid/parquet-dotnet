using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Encryption;
using Parquet.Extensions;

namespace Parquet {
    /// <summary>
    /// Base class for reader and writer
    /// </summary>
    public class ParquetActor {
#pragma warning disable IDE1006
        internal static readonly byte[] MagicBytes = Encoding.ASCII.GetBytes("PAR1");
        internal static readonly byte[] MagicBytesEncrypted = Encoding.ASCII.GetBytes("PARE");
#pragma warning restore IDE1006

        private readonly Stream _fileStream;

        private BinaryWriter? _binaryWriter;

        internal ParquetActor(Stream? fileStream) =>
            _fileStream = fileStream ?? throw new ArgumentNullException(nameof(fileStream));

        /// <summary>
        /// Original stream to write or read
        /// </summary>
        protected Stream Stream => _fileStream;

        internal bool IsEncryptedFile;

        internal BinaryWriter Writer => _binaryWriter ??= new BinaryWriter(_fileStream);

        /// <summary>
        /// Validates that this file is a valid parquet file by reading head and tail of it
        /// </summary>
        /// <returns></returns>
        /// <exception cref="IOException"></exception>
        public async Task ValidateFileAsync() {
            _fileStream.Seek(0, SeekOrigin.Begin);
            byte[] head = await _fileStream.ReadBytesExactlyAsync(4);

            _fileStream.Seek(-4, SeekOrigin.End);
            byte[] tail = await _fileStream.ReadBytesExactlyAsync(4);

            if(!MagicBytes.SequenceEqual(head) || !MagicBytes.SequenceEqual(tail)) {
                if(!MagicBytesEncrypted.SequenceEqual(head) || !MagicBytesEncrypted.SequenceEqual(tail)) {
                    throw new IOException($"not a parquet file, head: {head.ToHexString()}, tail: {tail.ToHexString()}");
                }
                IsEncryptedFile = true;
            }
        }

        internal async ValueTask<Meta.FileMetaData> ReadMetadataAsync(
            string? footerEncryptionKey = null,
            string? footerSigningKey = null,
            string? aadPrefix = null
        ) {
            // Move to tail and read the whole footer payload:
            int tailLen = await GoBeforeFooterAsync();
            byte[] tail = await Stream.ReadBytesExactlyAsync(tailLen);

            using var ms = new MemoryStream(tail, writable: false);
            var proto = new Parquet.Meta.Proto.ThriftCompactProtocolReader(ms);

            // -----------------------------
            // ENCRYPTED-FOOTER MODE (PARE)
            // -----------------------------
            if(IsEncryptedFile) {
                if(string.IsNullOrWhiteSpace(footerEncryptionKey)) {
                    throw new InvalidDataException($"{nameof(ParquetOptions.FooterEncryptionKey)} is required for files with encrypted footers.");
                }

                // Tail = FileCryptoMetaData || EncryptedFooterModule
                var decr = Encryption.EncryptionBase.CreateFromCryptoMeta(proto, footerEncryptionKey!, aadPrefix);
                byte[] plainFooter = decr.DecryptFooter(proto);

                using var fms = new MemoryStream(plainFooter, writable: false);
                var fpr = new Parquet.Meta.Proto.ThriftCompactProtocolReader(fms);
                var meta = Meta.FileMetaData.Read(fpr);
                meta.Decrypter = decr;           // needed so page/index readers decrypt modules
                return meta;
            }

            // ------------------------------------------
            // PLAINTEXT FOOTER (optionally signed §5.5)
            // Tail is either:
            //   [footer][len][PAR1]  (legacy, but in our "tail" we only have 'footer')
            //   or
            //   [footer][nonce(12)|tag(16)]  (we only see footer+28 here; the [len][PAR1] was stripped earlier)
            // ------------------------------------------

            // Try "signed plaintext footer" first if at least 28 bytes are available for nonce+tag.
            if(tailLen >= 28) {
                int footerLen = tailLen - 28;

                Meta.FileMetaData? metaSigned = null;
                try {
                    using var fmsProbe = new MemoryStream(tail, 0, footerLen, writable: false);
                    var rProbe = new Parquet.Meta.Proto.ThriftCompactProtocolReader(fmsProbe);
                    metaSigned = Meta.FileMetaData.Read(rProbe);
                } catch {
                    metaSigned = null; // couldn't parse — treat as legacy below
                }

                // Per spec, plaintext-footer-with-signature stores EncryptionAlgorithm in FileMetaData.
                if(metaSigned is not null && metaSigned.EncryptionAlgorithm is not null) {
                    Meta.EncryptionAlgorithm alg = metaSigned.EncryptionAlgorithm;
                    byte[] aadFileUnique;
                    bool requirePrefix;
                    byte[] aadPrefixBytes;

                    if(alg.AESGCMV1 is not null) {
                        aadFileUnique = alg.AESGCMV1.AadFileUnique ?? Array.Empty<byte>();
                        requirePrefix = alg.AESGCMV1.SupplyAadPrefix == true;
                        aadPrefixBytes = requirePrefix
                            ? (!string.IsNullOrEmpty(aadPrefix)
                                ? System.Text.Encoding.ASCII.GetBytes(aadPrefix!)
                                : throw new InvalidDataException("This file requires an AAD prefix to verify the footer signature."))
                            : (alg.AESGCMV1.AadPrefix ?? Array.Empty<byte>());
                    } else if(alg.AESGCMCTRV1 is not null) {
                        aadFileUnique = alg.AESGCMCTRV1.AadFileUnique ?? Array.Empty<byte>();
                        requirePrefix = alg.AESGCMCTRV1.SupplyAadPrefix == true;
                        aadPrefixBytes = requirePrefix
                            ? (!string.IsNullOrEmpty(aadPrefix)
                                ? System.Text.Encoding.ASCII.GetBytes(aadPrefix!)
                                : throw new InvalidDataException("This file requires an AAD prefix to verify the footer signature."))
                            : (alg.AESGCMCTRV1.AadPrefix ?? Array.Empty<byte>());
                    } else {
                        throw new InvalidDataException("Unsupported encryption algorithm for signed plaintext footer.");
                    }

                    // Pick signer to match the file’s algorithm (GCM vs GCM-CTR)
                    Encryption.EncryptionBase signer =
                        (alg.AESGCMV1 is not null)
                            ? new AES_GCM_V1_Encryption()
                            : new AES_GCM_CTR_V1_Encryption();

                    signer.AadFileUnique = aadFileUnique;
                    signer.AadPrefix = aadPrefixBytes;

                    if(string.IsNullOrWhiteSpace(footerSigningKey))
                        throw new InvalidDataException($"{nameof(ParquetOptions.FooterSigningKey)} is required to verify a signed plaintext footer.");

                    // Build AAD for the Footer module using the right variant
                    byte[] aad = signer.BuildAad(Meta.ParquetModules.Footer);

                    // Key to verify signature (same as parquet-mr footer key)
                    byte[] key = Encryption.EncryptionBase.ParseKeyString(footerSigningKey!);

                    // Stored values
                    byte[] footerBytes = tail.AsSpan(0, footerLen).ToArray();
                    byte[] nonce = new byte[12];
                    byte[] storedTag = new byte[16];
                    Buffer.BlockCopy(tail, footerLen + 0, nonce, 0, 12);
                    Buffer.BlockCopy(tail, footerLen + 12, storedTag, 0, 16);

                    // First try: “encrypt-then-tag” (footer as plaintext)
                    byte[] calcTag = new byte[16];
                    byte[] tmpCt = new byte[footerLen]; // ciphertext thrown away
                    CryptoHelpers.GcmEncryptOrThrow(key, nonce, footerBytes, tmpCt, calcTag, aad);

                    bool ok = CryptoHelpers.FixedTimeEquals(calcTag, storedTag);
                    EncTrace.VerifyAttempt("PF-Footer", "encrypt-then-tag", nonce, storedTag, ok);

                    if(!ok) {
                        // Second try: “AAD-only” tag (empty plaintext, AAD = parquetAAD || footer)
                        // i.e., authenticate footer bytes as part of AAD, with zero-length plaintext.
                        byte[] aad2 = new byte[aad.Length + footerBytes.Length];
                        Buffer.BlockCopy(aad, 0, aad2, 0, aad.Length);
                        Buffer.BlockCopy(footerBytes, 0, aad2, aad.Length, footerBytes.Length);

                        byte[] calcTag2 = new byte[16];
                        VerifyPlaintextFooterSignature(tail, key, nonce, calcTag2, aad2);

                        ok = CryptoHelpers.FixedTimeEquals(calcTag2, storedTag);
                        EncTrace.VerifyAttempt("PF-Footer", "AAD-only", nonce, storedTag, ok);
                    }
                    EncTrace.FooterMode("ReadMetadata", "PLAINTEXT_FOOTER");

                    if(!ok)
                        throw new InvalidDataException("Footer signature verification failed.");

                    // Success — in plaintext-footer mode, EncryptionAlgorithm is present to carry AAD
                    // for signature verification. Only require a footer key if any column actually
                    // uses footer-key encryption. If columns are encrypted *only* with column-keys,
                    // don't require a footer key; still create a decrypter with AAD so we can swap
                    // in column keys later.
                    bool hasAnyEncryptedCols =
                    (metaSigned.RowGroups != null) &&
                    metaSigned.RowGroups.Any(rg => rg.Columns != null &&
                    rg.Columns.Any(cc => cc.CryptoMetadata != null));
                    bool needsFooterKey =
                    hasAnyEncryptedCols &&
                    metaSigned.RowGroups!.Any(rg => rg.Columns!.Any(
                    cc => cc.CryptoMetadata?.ENCRYPTIONWITHFOOTERKEY != null));

                    if(!hasAnyEncryptedCols) {
                        metaSigned.Decrypter = null;
                    } else if(needsFooterKey) {
                        if(string.IsNullOrWhiteSpace(footerEncryptionKey))
                            throw new InvalidDataException($"{nameof(ParquetOptions.FooterEncryptionKey)} is required to read encrypted columns in plaintext-footer files.");
                        metaSigned.Decrypter = EncryptionBase.CreateFromAlgorithm(
                        metaSigned.EncryptionAlgorithm!,
                        footerEncryptionKey!,
                        aadPrefix
                        );
                    } else {
                        // Column-key only: build a decrypter with AAD; key will be supplied per-column.
                        EncryptionBase dec = (alg.AESGCMV1 is not null)
                            ? new AES_GCM_V1_Encryption()
                            : new AES_GCM_CTR_V1_Encryption();
                        dec.AadFileUnique = aadFileUnique;
                        dec.AadPrefix = aadPrefixBytes;
                        metaSigned.Decrypter = dec;
                    }

                    return metaSigned;
                }
            }

            // Legacy plaintext (no signature / no algorithm in footer)
            {
                using var fms = new MemoryStream(tail, writable: false);
                var fpr = new Parquet.Meta.Proto.ThriftCompactProtocolReader(fms);
                var meta = Meta.FileMetaData.Read(fpr);
                meta.Decrypter = null;
                return meta;
            }
        }

        internal async ValueTask<int> GoBeforeFooterAsync() {
            //go to -4 bytes (PAR1) -4 bytes (footer length number)
            _fileStream.Seek(-8, SeekOrigin.End);
            int footerLength = await _fileStream.ReadInt32Async();

            //set just before footer starts
            _fileStream.Seek(-8 - footerLength, SeekOrigin.End);

            return footerLength;
        }

        // inside ParquetActor
        internal static void VerifyPlaintextFooterSignature(
            ReadOnlySpan<byte> footerBytes,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> nonce12,
            ReadOnlySpan<byte> storedTag16,
            ReadOnlySpan<byte> moduleAad) {
            // We only need to reproduce the tag; ciphertext is ignored.
            Span<byte> tmpCt = footerBytes.Length == 0 ? Span<byte>.Empty : new byte[footerBytes.Length];
            Span<byte> calcTag = stackalloc byte[16];

            CryptoHelpers.GcmEncryptOrThrow(
                key: key.ToArray(),
                nonce: nonce12,
                plaintext: footerBytes,
                ciphertext: tmpCt,
                tag: calcTag,
                aad: moduleAad
            );

            if(!CryptoHelpers.FixedTimeEquals(calcTag, storedTag16))
                throw new InvalidDataException("Footer signature verification failed.");
        }
    }
}