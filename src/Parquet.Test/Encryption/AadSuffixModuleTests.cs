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
    public class AadSuffixModuleTests {
        private static readonly byte[] Key128 = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        private static readonly byte[] AadPrefix = Encoding.ASCII.GetBytes("aad_prefix_idx");
        private static readonly byte[] AadFileUnique = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        private const short RG = 2, COL = 4, PAGE = 0;

        private static ThriftCompactProtocolReader R(byte[] buf) => new ThriftCompactProtocolReader(new MemoryStream(buf));
        private static byte[] Le(short v) => BitConverter.GetBytes(v);

        private static byte[] FrameGcm(byte[] nonce12, byte[] ciphertext, byte[] tag16) {
            int len = nonce12.Length + ciphertext.Length + tag16.Length;
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(len), 0, 4);
            ms.Write(nonce12, 0, nonce12.Length);
            ms.Write(ciphertext, 0, ciphertext.Length);
            ms.Write(tag16, 0, tag16.Length);
            return ms.ToArray();
        }

        private static byte[] BuildAad(byte module, short? rg, short? col, short? page = null) {
            using var ms = new MemoryStream();
            ms.Write(AadPrefix, 0, AadPrefix.Length);
            // suffix:
            ms.Write(AadFileUnique, 0, AadFileUnique.Length);
            ms.WriteByte(module);
            if(rg.HasValue)
                ms.Write(Le(rg.Value), 0, 2);
            if(col.HasValue)
                ms.Write(Le(col.Value), 0, 2);
            if(page.HasValue)
                ms.Write(Le(page.Value), 0, 2);
            return ms.ToArray();
        }

        private static byte[] EncryptGcm(byte[] key, byte[] aad, byte[] plaintext, out byte[] nonce, out byte[] tag) {
            nonce = RandomNumberGenerator.GetBytes(12);
            tag = new byte[16];
            byte[] ct = new byte[plaintext.Length];
#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(key, 16);
#else
            using var gcm = new AesGcm(key);
#endif
            gcm.Encrypt(nonce, plaintext, ct, tag, aad);
            return ct;
        }

        [Theory]
        [InlineData(ParquetModules.ColumnIndex)]
        [InlineData(ParquetModules.OffsetIndex)]
        [InlineData(ParquetModules.BloomFilter_Header)]
        [InlineData(ParquetModules.BloomFilter_Bitset)]
        [InlineData(ParquetModules.ColumnMetaData)]
        public void NonPage_Modules_Decrypt_With_Correct_AAD(ParquetModules module) {
            byte[] plaintext = Encoding.ASCII.GetBytes($"ok-{module}");
            byte[] aad = BuildAad((byte)module, RG, COL);
            byte[] ct = EncryptGcm(Key128, aad, plaintext, out byte[]? nonce, out byte[]? tag);
            byte[] framed = FrameGcm(nonce, ct, tag);

            var enc = new AES_GCM_V1_Encryption {
                SecretKey = Key128,
                AadPrefix = AadPrefix,
                AadFileUnique = AadFileUnique
            };

            byte[] result = module switch {
                ParquetModules.ColumnIndex => enc.DecryptColumnIndex(R(framed), RG, COL),
                ParquetModules.OffsetIndex => enc.DecryptOffsetIndex(R(framed), RG, COL),
                ParquetModules.BloomFilter_Header => enc.BloomFilterHeader(R(framed), RG, COL),
                ParquetModules.BloomFilter_Bitset => enc.BloomFilterBitset(R(framed), RG, COL),
                ParquetModules.ColumnMetaData => enc.DecryptColumnMetaData(R(framed), RG, COL),
                _ => throw new NotSupportedException()
            };

            Assert.Equal(plaintext, result);
        }

        [Fact]
        public void Wrong_Ordinal_Endianness_Fails() {
            // Use ColumnIndex as an example; flip endianness of RG
            ParquetModules module = ParquetModules.ColumnIndex;
            byte[] plaintext = Encoding.ASCII.GetBytes("bad-aad");

            // Build a wrong AAD where row group is BE instead of LE
            using var ms = new MemoryStream();
            ms.Write(AadPrefix, 0, AadPrefix.Length);
            ms.Write(AadFileUnique, 0, AadFileUnique.Length);
            ms.WriteByte((byte)module);
            // WRONG: big-endian write of RG
            byte[] rgBE = BitConverter.GetBytes((ushort)RG);
            Array.Reverse(rgBE);
            ms.Write(rgBE, 0, 2);
            // correct LE for COL (so we isolate the failure to RG)
            ms.Write(Le(COL), 0, 2);
            byte[] wrongAad = ms.ToArray();

            byte[] ct = EncryptGcm(Key128, wrongAad, plaintext, out byte[]? nonce, out byte[]? tag);
            byte[] framed = FrameGcm(nonce, ct, tag);

            var enc = new AES_GCM_V1_Encryption {
                SecretKey = Key128,
                AadPrefix = AadPrefix,
                AadFileUnique = AadFileUnique
            };

            Assert.ThrowsAny<CryptographicException>(() => {
                enc.DecryptColumnIndex(R(framed), RG, COL);
            });
        }
    }
}
