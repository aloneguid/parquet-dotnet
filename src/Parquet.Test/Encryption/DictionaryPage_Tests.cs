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
    public class DictionaryPage_Tests {
        private static readonly byte[] Key16 = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        private static readonly byte[] Key32 = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
        private static readonly byte[] Prefix = Encoding.ASCII.GetBytes("dict");
        private static readonly byte[] Unique = new byte[] { 0x11, 0x22, 0x33, 0x44 };
        private const short RG = 1, COL = 2;

        [Theory]
        [InlineData(false)] // AES_GCM_V1
        [InlineData(true)]  // AES_GCM_CTR_V1
        public void DictionaryPageHeader_GCM_NoPageOrdinal(bool ctrVariant) {
            byte[] key = ctrVariant ? Key32 : Key16;
            byte[] plaintext = Encoding.ASCII.GetBytes("dict-hdr");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            // AAD = prefix || unique || module(Dictionary_PageHeader) || rg || col
            byte[] aad = Prefix
              .Concat(Unique)
              .Concat(new byte[] { (byte)ParquetModules.Dictionary_PageHeader })
              .Concat(BitConverter.GetBytes(RG))
              .Concat(BitConverter.GetBytes(COL))
              .ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(key, 16);
#else
      using var gcm = new AesGcm(key);
#endif
            byte[] ct = new byte[plaintext.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, plaintext, ct, tag, aad);

            byte[] framed = TestCryptoUtils.FrameGcm(nonce, ct, tag);

            EncryptionBase dec = ctrVariant
              ? new AES_GCM_CTR_V1_Encryption { SecretKey = key, AadPrefix = Prefix, AadFileUnique = Unique }
              : new AES_GCM_V1_Encryption { SecretKey = key, AadPrefix = Prefix, AadFileUnique = Unique };

            byte[] outBytes = dec.DecryptDictionaryPageHeader(TestCryptoUtils.R(framed), RG, COL);
            Assert.Equal(plaintext, outBytes);

            // Negative: pretending there is a page ordinal in AAD must fail
            byte[] aadWrong = aad.Concat(BitConverter.GetBytes((short)0)).ToArray();
            byte[] tag2 = new byte[16];
            byte[] ct2 = new byte[plaintext.Length];
            byte[] nonce2 = RandomNumberGenerator.GetBytes(12);
            gcm.Encrypt(nonce2, plaintext, ct2, tag2, aadWrong);
            byte[] framedWrong = TestCryptoUtils.FrameGcm(nonce2, ct2, tag2);
            Assert.ThrowsAny<CryptographicException>(() =>
              dec.DecryptDictionaryPageHeader(TestCryptoUtils.R(framedWrong), RG, COL));
        }

        [Fact]
        public void DictionaryPage_Body_AES_GCM_V1_uses_GCM_NoPageOrdinal() {
            var gcmAlg = new AES_GCM_V1_Encryption { SecretKey = Key16, AadPrefix = Prefix, AadFileUnique = Unique };
            byte[] plaintext = Enumerable.Range(0, 123).Select(i => (byte)i).ToArray();
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            // AAD for Dictionary Page body (GCM): prefix || unique || module(Dictionary_Page) || rg || col   (NO page)
            byte[] aad = Prefix
              .Concat(Unique)
              .Concat(new byte[] { (byte)ParquetModules.Dictionary_Page })
              .Concat(BitConverter.GetBytes(RG))
              .Concat(BitConverter.GetBytes(COL))
              .ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key16, 16);
#else
      using var gcm = new AesGcm(Key16);
#endif
            byte[] ct = new byte[plaintext.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, plaintext, ct, tag, aad);

            byte[] framed = TestCryptoUtils.FrameGcm(nonce, ct, tag);
            byte[] outBytes = gcmAlg.DecryptDictionaryPage(TestCryptoUtils.R(framed), RG, COL);
            Assert.Equal(plaintext, outBytes);
        }

        [Fact]
        public void DictionaryPage_Body_AES_GCM_CTR_V1_uses_CTR_NoAAD() {
            var ctrAlg = new AES_GCM_CTR_V1_Encryption { SecretKey = Key32, AadPrefix = Prefix, AadFileUnique = Unique };
            byte[] plaintext = Encoding.ASCII.GetBytes("dict-page-ctr");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            // Build 16B IV = nonce || 0x00000001
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
                aes.Key = Key32;
                using ICryptoTransform enc = aes.CreateEncryptor();
                ciphertext = XorCtr(enc, iv, plaintext);
            }

            byte[] framed = TestCryptoUtils.FrameCtr(nonce, ciphertext);
            byte[] outBytes = ctrAlg.DecryptDictionaryPage(TestCryptoUtils.R(framed), RG, COL);
            Assert.Equal(plaintext, outBytes);
        }

        private static byte[] XorCtr(ICryptoTransform ecbEncryptor, byte[] iv16, byte[] input) {
            byte[] counter = (byte[])iv16.Clone();
            byte[] output = new byte[input.Length];
            int i = 0;
            while(i < input.Length) {
                byte[] ks = new byte[16];
                ecbEncryptor.TransformBlock(counter, 0, 16, ks, 0);
                int n = Math.Min(16, input.Length - i);
                for(int j = 0; j < n; j++)
                    output[i + j] = (byte)(input[i + j] ^ ks[j]);
                for(int p = 15; p >= 12; p--)
                    if(++counter[p] != 0)
                        break; // big-endian increment
                i += n;
            }
            return output;
        }

        [Fact]
        public void DictionaryPage_Encrypted_As_Dictionary_Fails_When_Decoded_As_DataPage() {
            // Encrypt a tiny dict page body with GCM (AES_GCM_V1) and try to decrypt with DecryptDataPage (module=Data_Page)
            var enc = new AES_GCM_V1_Encryption { SecretKey = Key16, AadPrefix = Prefix, AadFileUnique = Unique };
            byte[] pt = Encoding.ASCII.GetBytes("mod-swap");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);
            byte[] aad = Prefix
              .Concat(Unique)
              .Concat(new byte[] { (byte)ParquetModules.Dictionary_Page })
              .Concat(BitConverter.GetBytes(RG))
              .Concat(BitConverter.GetBytes(COL))
              .ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key16, 16);
#else
  using var gcm = new AesGcm(Key16);
#endif
            byte[] ct = new byte[pt.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, pt, ct, tag, aad);
            byte[] framed = TestCryptoUtils.FrameGcm(nonce, ct, tag);

            Assert.ThrowsAny<CryptographicException>(() => {
                enc.DecryptDataPage(TestCryptoUtils.R(framed), RG, COL, pageOrdinal: 0); // wrong module id -> AAD mismatch
            });
        }
    }
}
