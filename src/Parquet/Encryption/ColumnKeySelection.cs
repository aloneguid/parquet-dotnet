// src/Parquet/Internal/ColumnKeySelection.cs
using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Meta;

namespace Parquet.Encryption {
    internal static class ColumnKeySelection {
        private static string PathToString(IReadOnlyList<string> path) =>
          path is { Count: > 0 } ? string.Join(".", path) : throw new InvalidDataException("PathInSchema missing.");

        /// Resolve as a *string* first (to match your Footer*Key parsing), then parse to bytes.
        private static byte[]? TryResolveKeyBytes(ColumnChunk chunk, ParquetOptions opts) {
            EncryptionWithColumnKey? ck = chunk.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY;
            if(ck is null)
                return null;                                // footer-key column

            List<string> path = ck.PathInSchema ?? chunk.MetaData?.PathInSchema
              ?? throw new InvalidDataException("Encrypted column missing PathInSchema.");
            byte[]? keyMetadata = ck.KeyMetadata;

            // 1) Preferred: user-provided resolver (path, key_metadata) -> keyString
            if(opts.ColumnKeyResolver is not null) {
                string? keyStr = opts.ColumnKeyResolver(path, keyMetadata);
                if(!string.IsNullOrEmpty(keyStr))
                    return EncryptionBase.ParseKeyString(keyStr!);
            }

            // 2) Fallback: ColumnKeys dictionary by dotted path
            if(opts.ColumnKeys is not null) {
                string pathStr = PathToString(path);
                if(opts.ColumnKeys.TryGetValue(pathStr, out ParquetOptions.ColumnKeySpec? spec))
                    return EncryptionBase.ParseKeyString(spec.Key);
            }

            return null;
        }

        /// Swap the decrypter's working key for this column, restore automatically on dispose.
        internal static IDisposable PushKeyFor(ColumnChunk chunk, ParquetOptions opts, EncryptionBase? decrypter) {
            if(decrypter is null)
                return Noop.Instance;

            byte[]? k = TryResolveKeyBytes(chunk, opts);
            if(k is null)
                return Noop.Instance;                        // footer key stays

            byte[] prev = decrypter.FooterEncryptionKey!;
            decrypter.FooterEncryptionKey = k;
            return new Restore(() => decrypter.FooterEncryptionKey = prev);
        }

        private sealed class Noop : IDisposable { public static readonly Noop Instance = new(); public void Dispose() { } }
        private sealed class Restore : IDisposable { private readonly Action _a; public Restore(Action a) => _a = a; public void Dispose() => _a(); }
    }
}
