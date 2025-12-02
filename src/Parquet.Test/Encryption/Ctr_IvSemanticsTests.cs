// src/Parquet.Test/Encryption/Ctr_IvSemanticsTests.cs
using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Parquet.Encryption;
using Parquet.Meta.Proto;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class Ctr_IvSemanticsTests {

        [Fact(DisplayName = "CTR page uses IV = nonce || 00000001 (ciphertext of zeroes equals keystream)")]
        public void Ctr_Iv_LastWord_Is_00000001() {
            byte[] key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
            var enc = new AES_GCM_CTR_V1_Encryption {
                FooterEncryptionKey = key,
                AadPrefix = Array.Empty<byte>(),
                AadFileUnique = new byte[] { 1, 2, 3, 4 }
            };

            // One-block plaintext of zeroes → ciphertext should equal keystream(IV)
            byte[] plain = new byte[16];
            byte[] framed = enc.EncryptDataPage(plain, rowGroupOrdinal: 0, columnOrdinal: 0, pageOrdinal: 0);

            var rdr = new ThriftCompactProtocolReader(new MemoryStream(framed, writable: false));
            // Frame: [len(4)][nonce(12)][ciphertext(N)]
            Span<byte> counterBytes = rdr.ReadBytesExactly(4);
            int total = BinaryPrimitives.ReadInt32LittleEndian(counterBytes);
            byte[] nonce = rdr.ReadBytesExactly(12);
            int ctLen = total - 12;
            byte[] ct = rdr.ReadBytesExactly(ctLen);

            // Build IV = nonce || 00000001 and compute keystream via AES-ECB
            byte[] iv = new byte[16];
            Buffer.BlockCopy(nonce, 0, iv, 0, 12);
            iv[12] = 0;
            iv[13] = 0;
            iv[14] = 0;
            iv[15] = 1;

            byte[] ks = new byte[16];
            using var aes = Aes.Create();
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.None;
            aes.Key = key;
            using ICryptoTransform e = aes.CreateEncryptor();
            e.TransformBlock(iv, 0, 16, ks, 0);

            Assert.True(ctLen >= 16, "Expected at least one block of ciphertext");
            Assert.Equal(ks, ct.Take(16).ToArray()); // ciphertext==keystream for zero block
        }

        [Fact(DisplayName = "CTR page framed with IV counter = 00000000 fails to decrypt to original")]
        public void Ctr_Iv_With_ZeroCounter_Does_Not_RoundTrip() {
            byte[] key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
            var enc = new AES_GCM_CTR_V1_Encryption {
                FooterEncryptionKey = key,
                AadPrefix = Array.Empty<byte>(),
                AadFileUnique = new byte[] { 9, 9, 9, 9 }
            };

            byte[] plain = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
            // Create a bogus CTR frame using IV with counter=0 instead of 1
            byte[] nonce = RandomNumberGenerator.GetBytes(12);
            byte[] ivBad = new byte[16];
            Buffer.BlockCopy(nonce, 0, ivBad, 0, 12); // last four = 0

            byte[] ct;
            using(var aes = Aes.Create()) {
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.None;
                aes.Key = key;
                using ICryptoTransform ecb = aes.CreateEncryptor();
                ct = XorWithCtr(ecb, ivBad, plain);
            }

            // Frame: [len][nonce][ciphertext]
            int len = 12 + ct.Length;
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(len), 0, 4);
            ms.Write(nonce, 0, 12);
            ms.Write(ct, 0, ct.Length);
            ms.Position = 0;

            // Decrypt expecting IV=nonce||00000001 → must not equal original 'plain'
            var rdr = new ThriftCompactProtocolReader(ms);
            byte[] outBytes = enc.DecryptDataPage(rdr, 0, 0, 0);
            Assert.NotEqual(plain, outBytes); // proves implementation expects counter=1
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
                for(int p = 15; p >= 12; p--)
                    if(++counter[p] != 0)
                        break; // big-endian inc
                i += n;
            }
            return output;
        }
    }
}
