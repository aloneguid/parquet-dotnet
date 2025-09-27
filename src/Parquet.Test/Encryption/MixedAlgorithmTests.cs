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
    public class MixedAlgorithmTests {
        private static readonly byte[] Key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
        private static readonly byte[] Prefix = Encoding.ASCII.GetBytes("mix");
        private static readonly byte[] Unique = new byte[] { 9, 9, 9, 9 };

        [Fact]
        public void PageHeader_GCM_and_Page_CTR_Both_Decrypt() {
            const short RG = 1, COL = 2, PAGE = 3;

            // --- Header (GCM) ---
            byte[] headerPlain = Encoding.ASCII.GetBytes("page-header-thrift");
            byte[] nonceH = RandomNumberGenerator.GetBytes(12);
            byte[] aadH = Prefix
                .Concat(Unique)
                .Concat(new byte[] { (byte)ParquetModules.Data_PageHeader })
                .Concat(BitConverter.GetBytes(RG))
                .Concat(BitConverter.GetBytes(COL))
                .Concat(BitConverter.GetBytes(PAGE))
                .ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key, 16);
#else
            using var gcm = new AesGcm(Key);
#endif
            byte[] ctH = new byte[headerPlain.Length];
            byte[] tagH = new byte[16];
            gcm.Encrypt(nonceH, headerPlain, ctH, tagH, aadH);
            byte[] framedHeader = TestCryptoUtils.FrameGcm(nonceH, ctH, tagH);

            // --- Page (CTR) ---
            byte[] pagePlain = Encoding.ASCII.GetBytes("page-bytes-ctr");
            byte[] nonceP = RandomNumberGenerator.GetBytes(12);
            byte[] iv = new byte[16];
            Buffer.BlockCopy(nonceP, 0, iv, 0, 12);
            iv[12] = 0;
            iv[13] = 0;
            iv[14] = 0;
            iv[15] = 1;

            byte[] ctP;
            using(var aes = Aes.Create()) {
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.None;
                aes.Key = Key;
                using ICryptoTransform encryptor = aes.CreateEncryptor();
                ctP = XorCtr(encryptor, iv, pagePlain);
            }
            byte[] framedPage = TestCryptoUtils.FrameCtr(nonceP, ctP);

            var enc = new AES_GCM_CTR_V1_Encryption {
                SecretKey = Key,
                AadPrefix = Prefix,
                AadFileUnique = Unique
            };

            byte[] headerOut = enc.DecryptDataPageHeader(TestCryptoUtils.R(framedHeader), RG, COL, PAGE);
            byte[] pageOut = enc.DecryptDataPage(TestCryptoUtils.R(framedPage), RG, COL, PAGE);

            Assert.Equal(headerPlain, headerOut);
            Assert.Equal(pagePlain, pageOut);
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
                        break;
                i += n;
            }
            return output;
        }
    }
}
