using System;
using System.Linq;
using System.Security.Cryptography;
using Parquet.Encryption;
using Parquet.Meta;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class AadSuffixModuleTests {
        [Theory]
        [InlineData(ParquetModules.ColumnIndex)]
        [InlineData(ParquetModules.OffsetIndex)]
        [InlineData(ParquetModules.BloomFilter_Header)]
        [InlineData(ParquetModules.BloomFilter_Bitset)]
        [InlineData(ParquetModules.ColumnMetaData)]
        public void NonPage_Modules_Decrypt_With_Correct_AAD(ParquetModules module) {
            byte[] key = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
            byte[] prefix = Encoding.ASCII.GetBytes("aad_prefix_idx");
            byte[] unique = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
            const short RG = 2, COL = 4;

            byte[] plaintext = Encoding.ASCII.GetBytes($"ok-{module}");
            byte[] aad = TestCryptoUtils.BuildAad(prefix, unique, module, RG, COL);
            byte[] nonce = RandomNumberGenerator.GetBytes(12);
            byte[] ct = new byte[plaintext.Length];
            byte[] tag = new byte[16];

            using AesGcm gcm = TestCryptoUtils.NewAesGcm(key);
            gcm.Encrypt(nonce, plaintext, ct, tag, aad);

            var enc = new AES_GCM_V1_Encryption { FooterEncryptionKey = key, AadPrefix = prefix, AadFileUnique = unique };
            byte[] framed = TestCryptoUtils.FrameGcm(nonce, ct, tag);

            byte[] result = module switch {
                ParquetModules.ColumnIndex => enc.DecryptColumnIndex(TestCryptoUtils.R(framed), RG, COL),
                ParquetModules.OffsetIndex => enc.DecryptOffsetIndex(TestCryptoUtils.R(framed), RG, COL),
                ParquetModules.BloomFilter_Header => enc.BloomFilterHeader(TestCryptoUtils.R(framed), RG, COL),
                ParquetModules.BloomFilter_Bitset => enc.BloomFilterBitset(TestCryptoUtils.R(framed), RG, COL),
                ParquetModules.ColumnMetaData => enc.DecryptColumnMetaData(TestCryptoUtils.R(framed), RG, COL),
                _ => throw new NotSupportedException()
            };

            Assert.Equal(plaintext, result);
        }

    }
}
