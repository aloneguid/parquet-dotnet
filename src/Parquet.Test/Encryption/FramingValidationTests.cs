// src/Parquet.Test/Encryption/FramingValidationTests.cs
using System;
using System.IO;
using System.Linq;
using Parquet.Encryption;
using Parquet.Meta.Proto;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class FramingValidationTests {
        private static readonly byte[] Key = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();

        [Fact]
        public void Gcm_Framing_Too_Short_Throws_InvalidData() {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(8), 0, 4); // impossible total (< 12 + 16)
            ms.Write(new byte[8], 0, 8);
            var enc = new AES_GCM_V1_Encryption { SecretKey = Key, AadFileUnique = new byte[] { 1 } };
            Assert.Throws<InvalidDataException>(() => enc.DecryptColumnIndex(TestCryptoUtils.R(ms.ToArray()), 0, 0));
        }

        [Fact]
        public void Ctr_Framing_Negative_Ciphertext_Throws_InvalidData() {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(10), 0, 4); // < 12 (nonce)
            ms.Write(new byte[10], 0, 10);
            var enc = new AES_GCM_CTR_V1_Encryption { SecretKey = Key, AadFileUnique = new byte[] { 1 } };
            Assert.Throws<InvalidDataException>(() => enc.DecryptDataPage(TestCryptoUtils.R(ms.ToArray()), 0, 0, 0));
        }

        [Fact]
        public void Gcm_Framing_Length_ClaimsMoreThanBuffer_Throws_InvalidData() {
            int claimed = 12 + 0 + 16 + 5; // pretend 5 extra ciphertext bytes that aren't there
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(claimed), 0, 4);
            ms.Write(new byte[12 + 0 + 16], 0, 12 + 16); // actually provide only nonce + tag
            var enc = new AES_GCM_V1_Encryption { SecretKey = Key, AadFileUnique = new byte[] { 2 } };
            Assert.Throws<InvalidDataException>(() => enc.DecryptColumnIndex(TestCryptoUtils.R(ms.ToArray()), 0, 0));
        }

        [Fact]
        public void Gcm_Framing_Tag_Truncated_Throws_InvalidData() {
            int total = 12 + 0 + 16;             // nonce + empty ct + full tag
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(total), 0, 4);
            ms.Write(new byte[12], 0, 12);       // nonce ok
            ms.Write(new byte[8], 0, 8);         // only half the tag present
            var enc = new AES_GCM_V1_Encryption { SecretKey = Key, AadFileUnique = new byte[] { 3 } };
            Assert.Throws<InvalidDataException>(() => enc.DecryptOffsetIndex(TestCryptoUtils.R(ms.ToArray()), 0, 0));
        }

        [Fact]
        public void Gcm_Framing_Nonce_Truncated_Throws_InvalidData() {
            int total = 12 + 0 + 16;             // claims full nonce+tag
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(total), 0, 4);
            ms.Write(new byte[10], 0, 10);       // only 10 bytes of nonce
            ms.Write(new byte[16], 0, 16);       // tag present
            var enc = new AES_GCM_V1_Encryption { SecretKey = Key, AadFileUnique = new byte[] { 4 } };
            Assert.Throws<InvalidDataException>(() => enc.DecryptColumnMetaData(TestCryptoUtils.R(ms.ToArray()), 0, 0));
        }

        [Fact]
        public void Ctr_Framing_Length_ClaimsMoreThanBuffer_Throws_InvalidData() {
            int claimed = 12 + 8; // claims 8 bytes of ciphertext
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(claimed), 0, 4);
            ms.Write(new byte[12 + 4], 0, 12 + 4); // but only provide 4
            var enc = new AES_GCM_CTR_V1_Encryption { SecretKey = Key, AadFileUnique = new byte[] { 5 } };
            Assert.Throws<InvalidDataException>(() => enc.DecryptDataPage(TestCryptoUtils.R(ms.ToArray()), 0, 0, 0));
        }

        [Fact]
        public void Ctr_Framing_Nonce_Truncated_Throws_InvalidData() {
            int total = 12; // minimum possible (nonce only, 0-byte ciphertext)
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(total), 0, 4);
            ms.Write(new byte[10], 0, 10); // not enough for 12B nonce
            var enc = new AES_GCM_CTR_V1_Encryption { SecretKey = Key, AadFileUnique = new byte[] { 6 } };
            Assert.Throws<InvalidDataException>(() => enc.DecryptDataPage(TestCryptoUtils.R(ms.ToArray()), 0, 0, 0));
        }
    }
}
