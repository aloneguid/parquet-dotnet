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
    public class KeyParsingTests : TestBase {

        [Theory]
        [InlineData(16)]
        [InlineData(24)]
        [InlineData(32)]
        public void Base64_Key_Sizes_Accepted(int size) {
            byte[] keyBytes = Enumerable.Range(1, size).Select(i => (byte)i).ToArray();
            byte[] fileUnique = new byte[] { 5, 6, 7, 8 };
            string keyString = Convert.ToBase64String(keyBytes);

            // build a minimal FileCryptoMetaData with AESGCMV1 to drive DecryptFooter’s selection path;
            // we don’t actually decrypt; we just want decrypter setup to parse the key.
            var meta = new Meta.FileCryptoMetaData {
                EncryptionAlgorithm = new Meta.EncryptionAlgorithm {
                    AESGCMV1 = new Meta.AesGcmV1 {
                        AadFileUnique = fileUnique,
                        SupplyAadPrefix = false,
                        AadPrefix = Array.Empty<byte>()
                    }
                }
            };

            using MemoryStream ms = BuildStreamWithMetaAndEmptyEncryptedFooter(meta, keyBytes, aadPrefix: null, fileUnique: fileUnique);

            // Serialize meta and then call AES_GCM_V1_Encryption.Decrypt(...) indirectly is overkill;
            // instead, directly validate the parser via the class: we simulate by constructing
            // and setting the key with ParseKeyString behavior exposed via DecryptFooter.
            // So we just assert ParseKeyString would accept Base64 by invoking the whole flow:
            var protoWriter = new ThriftCompactProtocolWriter(ms);
            meta.Write(protoWriter);
            ms.Position = 0;

            var reader = new ThriftCompactProtocolReader(ms);

            byte[] footer = EncryptionBase.DecryptFooter(reader, keyString, aadPrefix: null, out EncryptionBase? decrypter);

            // We don't care about 'footer' value here (we didn't pass a real footer),
            // we care that we got to this point without an ArgumentException on key size.
            Assert.NotNull(decrypter);
            Assert.True(decrypter is AES_GCM_V1_Encryption or AES_GCM_CTR_V1_Encryption);
        }

        [Fact]
        public void Hex_Key_256_Accepted() {
            byte[] keyBytes = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
            string keyString = BitConverter.ToString(keyBytes).Replace("-", string.Empty); // 64 hex chars
            byte[] fileUnique = new byte[] { 7, 7, 7, 7 };

            // 32 bytes (256-bit) hex
            var meta = new Meta.FileCryptoMetaData {
                EncryptionAlgorithm = new Meta.EncryptionAlgorithm {
                    AESGCMV1 = new Meta.AesGcmV1 {
                        AadFileUnique = fileUnique,
                        SupplyAadPrefix = false,
                        AadPrefix = Array.Empty<byte>()
                    }
                }
            };

            using MemoryStream ms = BuildStreamWithMetaAndEmptyEncryptedFooter(meta, keyBytes, aadPrefix: null, fileUnique: fileUnique);
            var w = new ThriftCompactProtocolWriter(ms);
            meta.Write(w);
            ms.Position = 0;

            var r = new ThriftCompactProtocolReader(ms);
            byte[] _ = EncryptionBase.DecryptFooter(r, keyString, aadPrefix: null, out EncryptionBase? _decr);
            Assert.NotNull(_decr);
        }

        [Fact]
        public void Utf8_16byte_Accepted() {
            string keyString = "1234567890abcdef"; // 16 bytes
            byte[] keyBytes = Encoding.UTF8.GetBytes(keyString);
            byte[] fileUnique = new byte[] { 9, 9, 9, 9 };

            var meta = new Meta.FileCryptoMetaData {
                EncryptionAlgorithm = new Meta.EncryptionAlgorithm {
                    AESGCMV1 = new Meta.AesGcmV1 {
                        AadFileUnique = fileUnique,
                        SupplyAadPrefix = false,
                        AadPrefix = Array.Empty<byte>()
                    }
                }
            };

            using MemoryStream ms = BuildStreamWithMetaAndEmptyEncryptedFooter(meta, keyBytes, aadPrefix: null, fileUnique: fileUnique);
            var w = new ThriftCompactProtocolWriter(ms);
            meta.Write(w);
            ms.Position = 0;

            var r = new ThriftCompactProtocolReader(ms);
            byte[] _ = EncryptionBase.DecryptFooter(r, keyString, aadPrefix: null, out EncryptionBase? _decr);
            Assert.NotNull(_decr);
        }

        [Theory]
        [InlineData("short")]                                 // 5 bytes UTF-8
        [InlineData("not-base64@@@@")]                       // invalid base64, not hex, len != 16/24/32
        [InlineData("AB")]                                   // 1 byte hex (odd length)
        [InlineData("00112233445566778899AABBCCDDEEFF00")]   // 17 bytes hex (34 chars)
        public void Invalid_Key_Formats_Throw(string key) {
            var meta = new Meta.FileCryptoMetaData {
                EncryptionAlgorithm = new Meta.EncryptionAlgorithm {
                    AESGCMV1 = new Meta.AesGcmV1 {
                        AadFileUnique = new byte[] { 1, 2, 3, 4 },
                        SupplyAadPrefix = false,
                        AadPrefix = Array.Empty<byte>()
                    }
                }
            };

            using var ms = new MemoryStream();
            var w = new ThriftCompactProtocolWriter(ms);
            meta.Write(w);
            ms.Position = 0;

            var r = new ThriftCompactProtocolReader(ms);

            Assert.Throws<ArgumentException>(() => {
                _ = EncryptionBase.DecryptFooter(r, key, aadPrefix: null, out EncryptionBase _);
            });
        }

        private static MemoryStream BuildStreamWithMetaAndEmptyEncryptedFooter(
    Meta.FileCryptoMetaData meta,
    byte[] key,
    byte[]? aadPrefix,
    byte[] fileUnique) {
            // 1) Serialize FileCryptoMetaData
            var ms = new MemoryStream();
            var w = new ThriftCompactProtocolWriter(ms);
            meta.Write(w);

            // 2) Build GCM-framed empty footer: len(=12+0+16) | nonce(12) | ciphertext(0) | tag(16)
            byte[] nonce = RandomNumberGenerator.GetBytes(12);
            byte[] tag = new byte[16];
            byte[] plaintext = Array.Empty<byte>();    // empty footer payload
            byte[] ciphertext = Array.Empty<byte>();   // empty since plaintext empty

            // AAD = aadPrefix || (fileUnique || moduleId(Footer))
            byte[] aadSuffix = fileUnique
                .Concat(new byte[] { (byte)ParquetModules.Footer })
                .ToArray();
            byte[] aad = (aadPrefix ?? Array.Empty<byte>()).Concat(aadSuffix).ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(key, 16);
#else
    using var gcm = new AesGcm(key);
#endif
            gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);

            int total = 12 + 0 + 16;
            ms.Write(BitConverter.GetBytes(total), 0, 4);
            ms.Write(nonce, 0, nonce.Length);
            ms.Write(tag, 0, tag.Length);

            ms.Position = 0;
            return ms;
        }

    }
}
