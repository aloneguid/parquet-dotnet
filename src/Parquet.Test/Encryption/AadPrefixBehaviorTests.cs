using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Parquet.Encryption;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class AadPrefixBehaviorTests {
        private static readonly byte[] Key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
        private static readonly byte[] FileUnique = new byte[] { 1, 2, 3, 4 };
        private const short RG = 0, COL = 0;

        private static byte[] FrameGcm(byte[] nonce12, byte[] ciphertext, byte[] tag16) {
            int len = nonce12.Length + ciphertext.Length + tag16.Length;
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(len), 0, 4);
            ms.Write(nonce12, 0, nonce12.Length);
            ms.Write(ciphertext, 0, ciphertext.Length);
            ms.Write(tag16, 0, tag16.Length);
            return ms.ToArray();
        }
        private static ThriftCompactProtocolReader R(byte[] buf) => new ThriftCompactProtocolReader(new MemoryStream(buf));

        [Fact]
        public void Uses_Stored_AadPrefix_When_SupplyAadPrefix_False() {
            byte[] storedPrefix = Encoding.ASCII.GetBytes("stored-prefix");
            byte[] plaintext = Encoding.ASCII.GetBytes("col-metadata");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            // AAD = storedPrefix || (fileUnique || module || RG || COL)
            byte[] aadSuffix = new byte[] { }
                .Concat(FileUnique)
                .Concat(new byte[] { (byte)ParquetModules.ColumnMetaData })
                .Concat(BitConverter.GetBytes(RG))
                .Concat(BitConverter.GetBytes(COL))
                .ToArray();
            byte[] aad = storedPrefix.Concat(aadSuffix).ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key, 16);
#else
            using var gcm = new AesGcm(Key);
#endif
            byte[] ct = new byte[plaintext.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, plaintext, ct, tag, aad);

            var enc = new AES_GCM_V1_Encryption {
                SecretKey = Key,
                AadPrefix = storedPrefix,        // stored value, not supplied
                AadFileUnique = FileUnique
            };

            byte[] framed = FrameGcm(nonce, ct, tag);
            byte[] outBytes = enc.DecryptColumnMetaData(R(framed), RG, COL);
            Assert.Equal(plaintext, outBytes);
        }

        [Fact]
        public void Missing_Supplied_AadPrefix_Fails_Cleanly() {
            // Build bytes with a *required* supplied prefix "runtime-prefix"
            byte[] suppliedPrefix = Encoding.ASCII.GetBytes("runtime-prefix");
            byte[] plaintext = Encoding.ASCII.GetBytes("offset-index");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            byte[] aadSuffix = new byte[] { }
                .Concat(FileUnique)
                .Concat(new byte[] { (byte)ParquetModules.OffsetIndex })
                .Concat(BitConverter.GetBytes(RG))
                .Concat(BitConverter.GetBytes(COL))
                .ToArray();
            byte[] aad = suppliedPrefix.Concat(aadSuffix).ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key, 16);
#else
            using var gcm = new AesGcm(Key);
#endif
            byte[] ct = new byte[plaintext.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, plaintext, ct, tag, aad);

            // Simulate caller forgetting to provide the supplied prefix (AadPrefix = null/empty)
            var enc = new AES_GCM_V1_Encryption {
                SecretKey = Key,
                AadPrefix = Array.Empty<byte>(),
                AadFileUnique = FileUnique
            };

            byte[] framed = FrameGcm(nonce, ct, tag);

            // Wrong AAD -> Auth tag mismatch (platform may throw AuthenticationTagMismatchException or CryptographicException)
            Assert.ThrowsAny<CryptographicException>(() => {
                enc.DecryptOffsetIndex(R(framed), RG, COL);
            });
        }
    }
}
