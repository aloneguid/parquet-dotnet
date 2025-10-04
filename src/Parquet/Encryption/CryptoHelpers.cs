using System;
using System.IO;
using System.Security.Cryptography;

namespace Parquet.Encryption {
    static class CryptoHelpers {
        // Parquet uses 128-bit (16-byte) GCM tags everywhere.
        private const int GcmTagSizeBytes = 16;

        public static void FillNonce12(byte[] nonce) {
#if NET6_0_OR_GREATER
            RandomNumberGenerator.Fill(nonce);
#else
            using var rng = RandomNumberGenerator.Create();
            rng.GetBytes(nonce);
#endif
        }

        public static void GcmEncryptOrThrow(
            byte[] key,
            ReadOnlySpan<byte> nonce,
            ReadOnlySpan<byte> plaintext,
            Span<byte> ciphertext,
            Span<byte> tag,
            ReadOnlySpan<byte> aad) {
#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(key, GcmTagSizeBytes);
            gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);
#elif NET7_0_OR_GREATER || NET6_0_OR_GREATER || NET5_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1
            using var gcm = new AesGcm(key);
            gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);
#else
            throw new PlatformNotSupportedException("AES-GCM is not available on netstandard2.0. Target netstandard2.1 or .NET 6+.");
#endif
        }

        public static void GcmDecryptOrThrow(
    byte[] key,
    ReadOnlySpan<byte> nonce,
    ReadOnlySpan<byte> ciphertext,
    ReadOnlySpan<byte> tag,
    Span<byte> plaintext,
    ReadOnlySpan<byte> aad) {
#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(key, GcmTagSizeBytes);
            gcm.Decrypt(nonce, ciphertext, tag, plaintext, aad);
#elif NET7_0_OR_GREATER || NET6_0_OR_GREATER || NET5_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1
            using var gcm = new AesGcm(key);
            gcm.Decrypt(nonce, ciphertext, tag, plaintext, aad);
#else
            throw new PlatformNotSupportedException("AES-GCM is not available on netstandard2.0. Target netstandard2.1 or .NET 6+.");
#endif
        }

        public static bool FixedTimeEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) {
#if NET6_0_OR_GREATER || NETSTANDARD2_1
            return CryptographicOperations.FixedTimeEquals(a, b);
#else
            if(a.Length != b.Length)
                return false;
            int diff = 0;
            for(int i = 0; i < a.Length; i++)
                diff |= a[i] ^ b[i];
            return diff == 0;
#endif
        }

        // Write a plaintext footer and append nonce|tag per PF spec.
        // AAD = BuildAad(Footer) || footerBytes; plaintext is empty.
        internal static (byte[] Nonce12, byte[] Tag16) ComputePlaintextFooterSignature(
            byte[] footerBytes, byte[] signingKey, byte[] parquetAad) {
            if(signingKey == null || signingKey.Length == 0)
                throw new InvalidDataException("Footer signing key is required.");

            // Nonce
            byte[] nonce = new byte[12];
            FillNonce12(nonce);

            // AAD' = parquetAAD || footerBytes
            byte[] aadPrime = new byte[parquetAad.Length + footerBytes.Length];
            Buffer.BlockCopy(parquetAad, 0, aadPrime, 0, parquetAad.Length);
            Buffer.BlockCopy(footerBytes, 0, aadPrime, parquetAad.Length, footerBytes.Length);

            // Tag over empty plaintext
            byte[] tag = new byte[16];
            CryptoHelpers.GcmEncryptOrThrow(
                signingKey,
                nonce,
                plaintext: ReadOnlySpan<byte>.Empty,
                ciphertext: Span<byte>.Empty,
                tag: tag.AsSpan(),
                aad: aadPrime);

            return (nonce, tag);
        }

        public static byte[] DeriveKeyFromUtf8(string baseKeyUtf8, string? label = null, int expectedLen = 16) {
            byte[] baseBytes = System.Text.Encoding.UTF8.GetBytes(baseKeyUtf8);
            using var sha256 = System.Security.Cryptography.SHA256.Create();
            sha256.TransformBlock(baseBytes, 0, baseBytes.Length, null, 0);
            sha256.TransformBlock(new byte[] { 0 }, 0, 1, null, 0);
            byte[] labelBytes = System.Text.Encoding.UTF8.GetBytes(label ?? "derived");
            sha256.TransformFinalBlock(labelBytes, 0, labelBytes.Length);
            byte[] full = sha256.Hash!;
            if(expectedLen is 16 or 24 or 32) {
                byte[] result = new byte[expectedLen];
                Buffer.BlockCopy(full, 0, result, 0, expectedLen);
                return result;
            }
            throw new ArgumentOutOfRangeException(nameof(expectedLen));
        }
    }
}