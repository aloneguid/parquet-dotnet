using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Interop.Inspector {
    /// <summary>
    /// Thin client around the encrypted-parquet inspector fat JAR.
    /// Ensures stdout is JSON-only; stderr is captured for diagnostics.
    /// </summary>
    public sealed class EncParquetInspectorClient {
        /// <summary>
        /// Full path to the inspector fat jar. If null, the client tries:
        /// 1) ENV: ENCRYPTED_PARQUET_INSPECTOR_JAR
        /// 2) ./build/libs/parquet-inspect-all.jar (relative to current dir)
        /// </summary>
        public string JarPath { get; }

        /// <summary>
        /// Java executable to use (default: "java").
        /// </summary>
        public string JavaExe { get; }

        public EncParquetInspectorClient(string? jarPath = null, string javaExe = "java") {
            JarPath = ResolveJar(jarPath)
                ?? throw new FileNotFoundException("Inspector JAR not found. Set ENCRYPTED_PARQUET_INSPECTOR_JAR or pass an absolute path.");
            JavaExe = javaExe;
        }

        static string? ResolveJar(string? path) {
            if(!string.IsNullOrWhiteSpace(path))
                return Path.GetFullPath(path);

            string? env = Environment.GetEnvironmentVariable("ENCRYPTED_PARQUET_INSPECTOR_JAR");
            if(!string.IsNullOrWhiteSpace(env))
                return Path.GetFullPath(env);

            string local = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, "build", "libs", "parquet-inspect-all.jar"));
            return File.Exists(local) ? local : null;
        }

        /// <summary>
        /// Runs the inspector on a local parquet file.
        /// </summary>
        /// <param name="parquetPath">Path to parquet file (absolute or relative).</param>
        /// <param name="footerKeyUtf8">UTF-8 key string as your writer expects ("footerKey-16byte").</param>
        /// <param name="aadPrefix">Optional AAD prefix—REQUIRED if the file was written with SupplyAadPrefix=true.</param>
        /// <param name="timeoutMs">Optional timeout. Default 60s.</param>
        public async Task<ParquetInspectJson> InspectAsync(
            string parquetPath,
            string footerKeyUtf8,
            string? aadPrefix = null,
            int timeoutMs = 60_000,
            CancellationToken cancellationToken = default) {
            if(string.IsNullOrWhiteSpace(parquetPath))
                throw new ArgumentException("parquetPath is required.", nameof(parquetPath));
            if(!File.Exists(parquetPath))
                throw new FileNotFoundException($"Parquet file not found: {parquetPath}", parquetPath);
            if(string.IsNullOrWhiteSpace(footerKeyUtf8))
                throw new ArgumentException("footerKeyUtf8 is required.", nameof(footerKeyUtf8));

            string args = aadPrefix == null
                ? $"-jar \"{JarPath}\" \"{parquetPath}\" {footerKeyUtf8}"
                : $"-jar \"{JarPath}\" \"{parquetPath}\" {footerKeyUtf8} {aadPrefix}";

            var psi = new ProcessStartInfo {
                FileName = JavaExe,
                Arguments = args,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };

            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

            string stdout, stderr;

            try {
                if(!proc.Start())
                    throw new InvalidOperationException("Failed to start Java process.");

                Task<string> so = proc.StandardOutput.ReadToEndAsync();
                Task<string> se = proc.StandardError.ReadToEndAsync();

#if NET6_0_OR_GREATER
                Task exited = proc.WaitForExitAsync(cancellationToken);
#else
                var exited = Task.Run(() => { proc.WaitForExit(); }, cancellationToken);
#endif
                Task completed = await Task.WhenAny(Task.Delay(timeoutMs, cancellationToken), exited).ConfigureAwait(false);
                if(completed != exited) {
                    try { if(!proc.HasExited) proc.Kill(); } catch { /* ignore */ }
                    throw new TimeoutException($"Inspector timed out after {timeoutMs} ms");
                }

                stdout = await so.ConfigureAwait(false);
                stderr = await se.ConfigureAwait(false);
            } finally {
                try { if(!proc.HasExited) proc.Kill(); } catch { /* ignore */ }
            }

            // Deserialize JSON from stdout (either success or error shape)
            ParquetInspectJson? json = JsonSerializer.Deserialize<ParquetInspectJson>(
                stdout,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if(json == null)
                throw new EncParquetInspectorException("Inspector returned non-JSON stdout.", stdout, stderr, proc.ExitCode);

            if(proc.ExitCode != 0 || json.Error != null)
                throw new EncParquetInspectorException(
                    $"Inspector failed: {json.Error} - {json.Message}",
                    stdout, stderr, proc.ExitCode);

            return json;
        }
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
        public string? Type { get; set; }                  // e.g., ENCRYPTED_FOOTER
        public string? Algorithm { get; set; }             // AES_GCM_V1 / AES_GCM_CTR_V1
        public bool? SupplyAadPrefix { get; set; }
        public bool? AadSuppliedAtRead { get; set; }
    }

    public sealed class SchemaInfo {
        public string? String { get; set; }
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
        public string? Type { get; set; }
        public string? Codec { get; set; }
        public long TotalSize { get; set; }
        public long DataPageOffset { get; set; }
        public long DictionaryPageOffset { get; set; }
        public bool HasDictionaryPage { get; set; }
        public bool IsEncrypted { get; set; }
    }

    public sealed class TotalsInfo {
        public int RowGroups { get; set; }
        public long RowCount { get; set; }
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
