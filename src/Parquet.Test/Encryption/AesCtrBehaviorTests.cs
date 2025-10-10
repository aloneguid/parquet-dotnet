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
    public class AesCtrBehaviorTests {
        private static readonly byte[] Key256 = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
        private static readonly byte[] AadPrefix = Encoding.ASCII.GetBytes("spec_ctr_split");
        private static readonly byte[] AadFileUnique = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        private const short RG = 5, COL = 9, PAGE = 1;

        private static byte[] AadSuffix(byte module, short? rg, short? col, short? page) {
            using var ms = new MemoryStream();
            ms.Write(AadFileUnique, 0, AadFileUnique.Length);
            ms.WriteByte(module);
            if(rg.HasValue)
                ms.Write(BitConverter.GetBytes(rg.Value), 0, 2);
            if(col.HasValue)
                ms.Write(BitConverter.GetBytes(col.Value), 0, 2);
            if(page.HasValue)
                ms.Write(BitConverter.GetBytes(page.Value), 0, 2);
            return ms.ToArray();
        }

        [Fact]
        public void DataPage_Uses_CTR_Framing_And_Decrypts() {
            byte[] plaintext = Encoding.ASCII.GetBytes("ctr-page-bytes");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            // Build 16-byte IV = nonce || 0x00000001
            byte[] iv = new byte[16];
            Buffer.BlockCopy(nonce, 0, iv, 0, 12);
            iv[12] = 0;
            iv[13] = 0;
            iv[14] = 0;
            iv[15] = 1;

            // Encrypt via CTR keystream (ECB)
            byte[] ciphertext;
            using(var aes = Aes.Create()) {
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.None;
                aes.Key = Key256;
                using ICryptoTransform enc = aes.CreateEncryptor();
                ciphertext = XorWithCtr(enc, iv, plaintext);
            }

            byte[] framed = TestCryptoUtils.FrameCtr(nonce, ciphertext);
            var encCtr = new AES_GCM_CTR_V1_Encryption {
                FooterEncryptionKey = Key256,
                AadPrefix = AadPrefix,
                AadFileUnique = AadFileUnique
            };

            byte[] outBytes = encCtr.DecryptDataPage(TestCryptoUtils.R(framed), RG, COL, PAGE);
            Assert.Equal(plaintext, outBytes);
        }

        [Fact]
        public void DataPageHeader_Stays_GCM_And_Decrypts() {
            byte[] plaintext = Encoding.ASCII.GetBytes("gcm-header");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            byte[] aad = AadPrefix.Concat(AadSuffix((byte)ParquetModules.Data_PageHeader, RG, COL, PAGE)).ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key256, 16);
#else
            using var gcm = new AesGcm(Key256);
#endif
            byte[] ct = new byte[plaintext.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, plaintext, ct, tag, aad);

            byte[] framed = TestCryptoUtils.FrameGcm(nonce, ct, tag);

            var encCtr = new AES_GCM_CTR_V1_Encryption {
                FooterEncryptionKey = Key256,
                AadPrefix = AadPrefix,
                AadFileUnique = AadFileUnique
            };

            byte[] outBytes = encCtr.DecryptDataPageHeader(TestCryptoUtils.R(framed), RG, COL, PAGE);
            Assert.Equal(plaintext, outBytes);
        }

        [Fact]
        public void DataPageHeader_With_CTR_Framing_Is_Rejected() {
            // GCM is required for headers; feeding CTR-framed bytes should fail the GCM framing checks
            byte[] bogusHdr = Encoding.ASCII.GetBytes("hdr");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            // Make CTR ciphertext
            byte[] iv = new byte[16];
            Buffer.BlockCopy(nonce, 0, iv, 0, 12);
            iv[12] = 0;
            iv[13] = 0;
            iv[14] = 0;
            iv[15] = 1;
            byte[] ciphertext;
            using(var aes = Aes.Create()) {
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.None;
                aes.Key = Key256;
                using ICryptoTransform enc = aes.CreateEncryptor();
                ciphertext = XorWithCtr(enc, iv, bogusHdr);
            }

            byte[] framed = TestCryptoUtils.FrameCtr(nonce, ciphertext);
            var encCtr = new AES_GCM_CTR_V1_Encryption {
                FooterEncryptionKey = Key256,
                AadPrefix = AadPrefix,
                AadFileUnique = AadFileUnique
            };

            Assert.Throws<InvalidDataException>(() => {
                encCtr.DecryptDataPageHeader(TestCryptoUtils.R(framed), RG, COL, PAGE);
            });
        }

        private static byte[] XorWithCtr(ICryptoTransform ecbEncryptor, byte[] iv16, byte[] input) {
            byte[] counter = (byte[])iv16.Clone();
            byte[] output = new byte[input.Length];
            int i = 0;
            while(i < input.Length) {
                byte[] ks = new byte[16];
                ecbEncryptor.TransformBlock(counter, 0, 16, ks, 0);

                int n = Math.Min(16, input.Length - i);
                for(int j = 0; j < n; j++)
                    output[i + j] = (byte)(input[i + j] ^ ks[j]);

                // increment last 4 bytes (big-endian)
                for(int p = 15; p >= 12; p--)
                    if(++counter[p] != 0)
                        break;
                i += n;
            }
            return output;
        }

        // Utility: fresh decryptor with CTR variant
        private static AES_GCM_CTR_V1_Encryption Ctr() => new AES_GCM_CTR_V1_Encryption {
            FooterEncryptionKey = Key256,
            AadPrefix = Encoding.ASCII.GetBytes("ctr-multi"),
            AadFileUnique = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE }
        };


        [Theory]
        [InlineData(1)]             // tiny
        [InlineData(15)]            // just under block
        [InlineData(16)]            // exact block
        [InlineData(17)]            // crosses block
        [InlineData((4 * 1024) + 7)]  // not multiple of 16, multiple blocks
        [InlineData(256 * 1024)]    // 256 KiB many blocks
        public void DataPage_CTR_RoundTrips_VariousSizes(int size) {
            AES_GCM_CTR_V1_Encryption encCtr = Ctr();
            byte[] plain = new byte[size];
            for(int i = 0; i < plain.Length; i++)
                plain[i] = (byte)(i & 0xFF);

            // Use writer API to ensure nonce/IV framing is correct
            byte[] framed = encCtr.EncryptDataPage(plain, rowGroupOrdinal: 0, columnOrdinal: 0, pageOrdinal: 0);

            var r = new ThriftCompactProtocolReader(new MemoryStream(framed));
            byte[] outBytes = encCtr.DecryptDataPage(r, 0, 0, 0);

            Assert.Equal(plain, outBytes);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(3)]
        [InlineData(31)]
        [InlineData(32)]
        public void DictionaryPage_CTR_RoundTrips_VariousSizes(int size) {
            AES_GCM_CTR_V1_Encryption encCtr = Ctr();
            byte[] plain = new byte[size];
            for(int i = 0; i < plain.Length; i++)
                plain[i] = (byte)((i * 7) & 0xFF);

            byte[] framed = encCtr.EncryptDictionaryPage(plain, rowGroupOrdinal: 1, columnOrdinal: 2);

            var r = new ThriftCompactProtocolReader(new MemoryStream(framed));
            byte[] outBytes = encCtr.DecryptDictionaryPage(r, 1, 2);

            Assert.Equal(plain, outBytes);
        }

        [Fact]
        public void DataPage_CTR_ZeroLength_Is_Valid() {
            AES_GCM_CTR_V1_Encryption encCtr = Ctr();
            byte[] framed = encCtr.EncryptDataPage(Array.Empty<byte>(), rowGroupOrdinal: 2, columnOrdinal: 3, pageOrdinal: 4);

            var r = new ThriftCompactProtocolReader(new MemoryStream(framed));
            byte[] outBytes = encCtr.DecryptDataPage(r, 2, 3, 4);

            Assert.Empty(outBytes);
        }
    }
}
