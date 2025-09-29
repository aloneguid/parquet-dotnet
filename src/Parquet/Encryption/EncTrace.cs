// Parquet/Encryption/EncTrace.cs
using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Parquet.Encryption {
    internal static class EncTrace {
        // Turn on with: PARQUET_ENC_TRACE=1
        private static readonly bool Enabled =
            string.Equals(Environment.GetEnvironmentVariable("PARQUET_ENC_TRACE"), "1", StringComparison.OrdinalIgnoreCase);

        internal static void Log(string msg) {
#if DEBUG
            if(Enabled)
                System.Console.Error.WriteLine($"[parquet-enc] {msg}");
#endif
        }

        internal static string Hex(ReadOnlySpan<byte> s, int max = 48) {
            if(s.IsEmpty)
                return "(empty)";
            int n = Math.Min(s.Length, max);
            var sb = new StringBuilder((n * 2) + 16);
            for(int i = 0; i < n; i++)
                sb.AppendFormat("{0:x2}", s[i]);
            if(s.Length > n)
                sb.Append($"..(+{s.Length - n})");
            return sb.ToString();
        }

        internal static string KeyId(ReadOnlySpan<byte> key) {
            // Don’t dump keys; show length + short fingerprint for correlation
            using var sha = SHA256.Create();
            byte[] d = sha.ComputeHash(key.ToArray());
            return $"len={key.Length}, sha256={Hex(d.AsSpan(0, 8))}";
        }

        internal static void FrameGcm(string where, Meta.ParquetModules m, int totalLen,
                                      ReadOnlySpan<byte> nonce12, int ctLen, ReadOnlySpan<byte> tag16,
                                      short? rg, short? col, short? pg) {
            Log($"{where} GCM frame m={m} len={totalLen} ct={ctLen} nonce={Hex(nonce12, 12)} tag={Hex(tag16, 16)} rg={rg} col={col} pg={pg}");
        }

        internal static void FrameCtr(string where, Meta.ParquetModules m, int totalLen,
                                      ReadOnlySpan<byte> nonce12, int ctLen,
                                      short? rg, short? col, short? pg) {
            Log($"{where} CTR frame m={m} len={totalLen} ct={ctLen} nonce={Hex(nonce12, 12)} rg={rg} col={col} pg={pg}");
        }

        internal static void Aad(string where, ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> fileUnique,
                                 ReadOnlySpan<byte> aad, Meta.ParquetModules m,
                                 short? rg, short? col, short? pg) {
            Log($"{where} AAD m={m} rg={rg} col={col} pg={pg} prefix=({prefix.Length}) {Hex(prefix)} fileUnique={Hex(fileUnique)} fullAAD=({aad.Length}) {Hex(aad)}");
        }

        internal static void Alg(string where, string alg, bool supplyPrefix, bool hasStoredPrefix) {
            Log($"{where} alg={alg} supplyPrefix={supplyPrefix} storedPrefix={(hasStoredPrefix ? "yes" : "no")}");
        }

        internal static void FooterMode(string where, string mode) {
            Log($"{where} footerMode={mode}");
        }

        internal static void VerifyAttempt(string where, string variant, ReadOnlySpan<byte> nonce12, ReadOnlySpan<byte> tag16, bool ok) {
            Log($"{where} verify={variant} nonce={Hex(nonce12, 12)} tag={Hex(tag16, 16)} ok={ok}");
        }
    }
}
