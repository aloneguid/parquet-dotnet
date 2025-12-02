using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Interop.Inspector {
    /// <summary>
    /// Thin client around the encrypted-parquet inspector fat JAR.
    /// Ensures stdout is JSON-only; stderr is captured for diagnostics.
    /// </summary>
    public sealed class EncParquetInspectorClient {
        /// <summary>Full path to the inspector fat jar.</summary>
        public string JarPath { get; }

        /// <summary>Java executable to use (default: "java").</summary>
        public string JavaExe { get; }

        public EncParquetInspectorClient(string? jarPath = null, string javaExe = "java") {
            string toolsPath = Path.GetFullPath(Path.Combine("..", "..", "..", "..", "..", "tools"));
            string defaultJar = Path.Combine(toolsPath, "encrypted_parquet_inspector-1.0.3.jar");
            jarPath ??= defaultJar;
            JarPath = ResolveJar(jarPath)
                ?? throw new FileNotFoundException(
                    "Inspector JAR not found. Set ENCRYPTED_PARQUET_INSPECTOR_JAR or pass an absolute path.");
            JavaExe = javaExe;
        }

        static string? ResolveJar(string? path) {
            if(!string.IsNullOrWhiteSpace(path)) {
                string full = Path.GetFullPath(path);
                if(File.Exists(full))
                    return full;
            }

            string? env = Environment.GetEnvironmentVariable("ENCRYPTED_PARQUET_INSPECTOR_JAR");
            if(!string.IsNullOrWhiteSpace(env)) {
                string full = Path.GetFullPath(env);
                if(File.Exists(full))
                    return full;
            }

            return null;
        }

        /// <summary>
        /// Runs the inspector on a local parquet file using a UTF-8 key string.
        /// Auto-detects hex-looking keys and passes --hex; override via <paramref name="footerKeyIsHex"/>.
        /// </summary>
        public Task<ParquetInspectJson> InspectAsync(
            string parquetPath,
            string footerKeyUtf8OrHex,
            string? aadPrefix = null,
            int timeoutMs = 60_000,
            CancellationToken cancellationToken = default,
            bool? footerKeyIsHex = null)
            => InspectCoreAsync(
                parquetPath,
                footerKeyUtf8OrHex,
                aadPrefixUtf8OrHex: aadPrefix,
                footerKeyBytes: null,
                aadPrefixBytes: null,
                timeoutMs,
                cancellationToken,
                footerKeyIsHex);

        /// <summary>
        /// Runs the inspector, passing a raw key as bytes. AAD prefix can be passed as bytes too.
        /// Bytes are sent as 0xHEX to the JVM for exact fidelity.
        /// </summary>
        public Task<ParquetInspectJson> InspectAsync(
            string parquetPath,
            byte[] footerKey,
            byte[]? aadPrefix = null,
            int timeoutMs = 60_000,
            CancellationToken cancellationToken = default)
            => InspectCoreAsync(
                parquetPath,
                footerKeyUtf8OrHex: null,
                aadPrefixUtf8OrHex: null,
                footerKeyBytes: footerKey,
                aadPrefixBytes: aadPrefix,
                timeoutMs,
                cancellationToken,
                footerKeyIsHex: true); // we send as 0xHEX; inspector will read with --hex

        private async Task<ParquetInspectJson> InspectCoreAsync(
            string parquetPath,
            string? footerKeyUtf8OrHex,
            string? aadPrefixUtf8OrHex,
            byte[]? footerKeyBytes,
            byte[]? aadPrefixBytes,
            int timeoutMs,
            CancellationToken cancellationToken,
            bool? footerKeyIsHex) {
            if(string.IsNullOrWhiteSpace(parquetPath))
                throw new ArgumentException("parquetPath is required.", nameof(parquetPath));
            if(!File.Exists(parquetPath))
                throw new FileNotFoundException($"Parquet file not found: {parquetPath}", parquetPath);

            // Build args
            IReadOnlyList<string> args = BuildArgs(
                JarPath,
                parquetPath,
                footerKeyUtf8OrHex,
                aadPrefixUtf8OrHex,
                footerKeyBytes,
                aadPrefixBytes,
                footerKeyIsHex);

            var psi = new ProcessStartInfo {
                FileName = JavaExe,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
#if NET6_0_OR_GREATER
            foreach(string a in args)
                psi.ArgumentList.Add(a);
#else
            psi.Arguments = JoinArgs(args);
#endif

            using var proc = new Process { StartInfo = psi, EnableRaisingEvents = false };

            string stdout, stderr;
            try {
                if(!proc.Start())
                    throw new InvalidOperationException("Failed to start Java process.");

                Task<string> so = proc.StandardOutput.ReadToEndAsync();
                Task<string> se = proc.StandardError.ReadToEndAsync();

#if NET6_0_OR_GREATER
                await proc.WaitForExitAsync(cancellationToken).WaitAsync(TimeSpan.FromMilliseconds(timeoutMs), cancellationToken)
                    .ConfigureAwait(false);
#else
                if (!proc.WaitForExit(timeoutMs)) {
                    try { if (!proc.HasExited) proc.Kill(); } catch { /* ignore */ }
                    throw new TimeoutException($"Inspector timed out after {timeoutMs} ms");
                }
#endif
                stdout = await so.ConfigureAwait(false);
                stderr = await se.ConfigureAwait(false);
            } finally {
                try { if(!proc.HasExited) proc.Kill(); } catch { /* ignore */ }
            }

            // Deserialize JSON from stdout (either success or error shape)
            ParquetInspectJson? json = JsonSerializer.Deserialize<ParquetInspectJson>(
                stdout,
                new JsonSerializerOptions {
                    PropertyNameCaseInsensitive = true,
                    ReadCommentHandling = JsonCommentHandling.Skip,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString
                });

            if(json == null)
                throw new EncParquetInspectorException("Inspector returned non-JSON stdout.", stdout, stderr, proc.ExitCode);

            if(proc.ExitCode != 0 || json.Error != null)
                throw new EncParquetInspectorException(
                    $"Inspector failed: {json.Error} - {json.Message}",
                    stdout, stderr, proc.ExitCode);

            return json;
        }

        static IReadOnlyList<string> BuildArgs(
            string jarPath,
            string parquetPath,
            string? footerKeyUtf8OrHex,
            string? aadPrefixUtf8OrHex,
            byte[]? footerKeyBytes,
            byte[]? aadPrefixBytes,
            bool? footerKeyIsHex) {
            var list = new List<string> { "-jar", jarPath, parquetPath };

            // Key argument & --hex flag handling
            string keyArg;
            bool hex = footerKeyIsHex ?? false;

            if(footerKeyBytes != null) {
                keyArg = "0x" + BytesToHex(footerKeyBytes);
                hex = true;
            } else if(!string.IsNullOrEmpty(footerKeyUtf8OrHex)) {
                keyArg = footerKeyUtf8OrHex!;
                if(footerKeyIsHex is null) {
                    // auto-detect 16/24/32-byte AES key in hex
                    hex = LooksLikeRawAesHex(keyArg);
                }
            } else {
                throw new ArgumentException("footer key is required (string or bytes).");
            }

            list.Add(keyArg);

            // Positional AAD prefix (the inspector accepts positional or --aadPrefix=)
            if(aadPrefixBytes != null) {
                list.Add("0x" + BytesToHex(aadPrefixBytes));
            } else if(!string.IsNullOrEmpty(aadPrefixUtf8OrHex)) {
                list.Add(aadPrefixUtf8OrHex!);
            }

            if(hex)
                list.Add("--hex");

            return list;
        }

        static bool LooksLikeRawAesHex(string s) {
            if(string.IsNullOrEmpty(s))
                return false;
            int n = s.Length;
            if(n != 32 && n != 48 && n != 64)
                return false;
            for(int i = 0; i < n; i++) {
                char c = s[i];
                bool isHex = (c >= '0' && c <= '9')
                           || (c >= 'a' && c <= 'f')
                           || (c >= 'A' && c <= 'F');
                if(!isHex)
                    return false;
            }
            return true;
        }

        static string BytesToHex(ReadOnlySpan<byte> bytes) {
            var sb = new StringBuilder(bytes.Length * 2);
            for(int i = 0; i < bytes.Length; i++)
                sb.Append(bytes[i].ToString("x2"));
            return sb.ToString();
        }

#if !NET6_0_OR_GREATER
        static string JoinArgs(IReadOnlyList<string> args) {
            // Minimal quoting for cross-platform; Java reads them as plain strings.
            var sb = new StringBuilder();
            for (int i = 0; i < args.Count; i++) {
                if (i > 0) sb.Append(' ');
                var a = args[i];
                if (a.Length == 0 || a.IndexOfAny(new[] { ' ', '\t', '"', '\'' }) >= 0) {
                    // quote with double-quotes; escape embedded quotes
                    sb.Append('"').Append(a.Replace("\"", "\\\"")).Append('"');
                } else {
                    sb.Append(a);
                }
            }
            return sb.ToString();
        }
#endif
    }

    // ------------- Models (match inspector JSON) -------------

    public sealed class ParquetInspectJson {
        // Error fields (present on failure)
        public string? Error { get; set; }
        public string? Message { get; set; }

        // Success fields
        public string? File { get; set; }
        public EncryptionInfo? Encryption { get; set; }
        public SchemaInfo? Schema { get; set; }
        public List<RowGroupInfo>? RowGroups { get; set; }
        public TotalsInfo? Totals { get; set; }
    }

    public sealed class EncryptionInfo {
        public string? Type { get; set; }                      // ENCRYPTED_FOOTER | PLAINTEXT_FOOTER
        public string? FooterMode { get; set; }                // same as Type
        public string? Algorithm { get; set; }                 // AES_GCM_V1 | AES_GCM_CTR_V1
        [JsonPropertyName("createdBy")] public string? CreatedBy { get; set; }

        // AAD + storage
        [JsonPropertyName("supplyAadPrefixFlag")] public bool? SupplyAadPrefixFlag { get; set; } // writer flag
        public bool? AadSuppliedAtRead { get; set; }           // what we passed to inspector
        public bool? HasStoredAadPrefix { get; set; }
        public string? StoredAadPrefixHex { get; set; }
        public bool? AadPrefixMatchesSupplied { get; set; }

        // Footer key metadata
        public bool? HasFooterKeyId { get; set; }
        public string? FooterKeyId { get; set; }
        public int? FooterKeyLengthBits { get; set; }

        // Inference
        public string? InferredLayout { get; set; }            // plaintext | uniform | partial
        public string? InferredAadMode { get; set; }           // none | stored | supply
        public bool? FooterSigned { get; set; }

        // Discovery/aux arrays
        [JsonPropertyName("encounteredColumnKeyIds")] public List<string>? EncounteredColumnKeyIds { get; set; }
        [JsonPropertyName("efFooterKeyColumns")] public List<string>? EfFooterKeyColumns { get; set; }
        [JsonPropertyName("pfFooterKeyColumns")] public List<string>? PfFooterKeyColumns { get; set; }
        [JsonPropertyName("columnKeyMetadataPF")] public List<ColumnKeyMetadataPfEntry>? ColumnKeyMetadataPF { get; set; }
    }

    public sealed class ColumnKeyMetadataPfEntry {
        public int RowGroup { get; set; }
        public string? Path { get; set; }
        public bool EncryptedWithFooterKey { get; set; } // should be false for PF column-key entries
        public string? ColumnKeyId { get; set; }
        public string? ColumnKeyIdHex { get; set; }
    }

    public sealed class SchemaInfo {
        [JsonPropertyName("string")] public string? String { get; set; }
        public List<FieldInfo>? Fields { get; set; }
    }

    public sealed class FieldInfo {
        public string? Name { get; set; }
        public string? Repetition { get; set; }
        public string? OriginalType { get; set; }
        public bool Primitive { get; set; }
        public string? PrimitiveTypeName { get; set; }
    }

    public sealed class RowGroupInfo {
        public int Index { get; set; }
        public long RowCount { get; set; }
        public long TotalByteSize { get; set; }
        public List<ColumnInfo>? Columns { get; set; }
    }

    public sealed class ColumnInfo {
        public string? Path { get; set; }
        public string? Type { get; set; }                      // may be null if decryptor not used
        public string? Codec { get; set; }                     // may be null if decryptor not used
        public long? TotalSize { get; set; }
        public long? DataPageOffset { get; set; }
        public long? DictionaryPageOffset { get; set; }
        public bool? HasDictionaryPage { get; set; }
        public bool IsEncrypted { get; set; }

        // Extra crypto details (if available)
        public bool? EncryptedWithFooterKey { get; set; }
        public string? ColumnKeyId { get; set; }
        public string? ColumnKeyIdHex { get; set; }
    }

    public sealed class TotalsInfo {
        public int RowGroups { get; set; }
        public long RowCount { get; set; }

        // Extras from enhanced inspector
        public int? ColumnsSeen { get; set; }
        public int? EncryptedColumns { get; set; }
        public int? PlaintextColumns { get; set; }
        public bool? AnyColumnKeyMetadata { get; set; }
        public bool? AnyEncryptedWithFooterKey { get; set; }
    }

    // ------------- Exception -------------

    public sealed class EncParquetInspectorException : Exception {
        public string Stdout { get; }
        public string Stderr { get; }
        public int ExitCode { get; }

        public EncParquetInspectorException(string message, string stdout, string stderr, int exitCode)
            : base(message) {
            Stdout = stdout;
            Stderr = stderr;
            ExitCode = exitCode;
        }

        public override string ToString()
            => $"{Message}\nExitCode={ExitCode}\nSTDERR:\n{Stderr}\nSTDOUT:\n{Stdout}";
    }
}
