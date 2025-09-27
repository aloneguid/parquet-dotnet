using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Parquet.Encryption;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class EncryptionPrimitives_WriteTests {
        private static readonly byte[] Key16 = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        private static readonly byte[] Key32 = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
        private static readonly byte[] Prefix = Encoding.ASCII.GetBytes("writer-aad");
        private static readonly byte[] Unique = new byte[] { 0x10, 0x20, 0x30, 0x40 };

        private static ThriftCompactProtocolReader R(byte[] buf)
            => new ThriftCompactProtocolReader(new MemoryStream(buf));

        private static AES_GCM_V1_Encryption MakeGcm(byte[] key) => new AES_GCM_V1_Encryption {
            SecretKey = key,
            AadPrefix = Prefix,
            AadFileUnique = Unique
        };

        private static AES_GCM_CTR_V1_Encryption MakeCtr(byte[] key) => new AES_GCM_CTR_V1_Encryption {
            SecretKey = key,
            AadPrefix = Prefix,
            AadFileUnique = Unique
        };

        [Fact]
        public void Gcm_Footer_Encrypt_RoundTrip() {
            AES_GCM_V1_Encryption enc = MakeGcm(Key32);
            AES_GCM_V1_Encryption dec = MakeGcm(Key32);
            byte[] plain = Encoding.ASCII.GetBytes("footer-bytes");

            // encrypt
            byte[] framed = enc.EncryptFooter(plain);

            // decrypt
            byte[] outBytes = dec.DecryptFooter(R(framed));
            Assert.Equal(plain, outBytes);
        }

        [Fact]
        public void Gcm_DataPageHeader_Encrypt_RoundTrip_With_PageOrdinal() {
            AES_GCM_V1_Encryption enc = MakeGcm(Key16);
            AES_GCM_V1_Encryption dec = MakeGcm(Key16);
            byte[] plain = Encoding.ASCII.GetBytes("hdr-0");

            short rg = 2, col = 3, page = 0;
            byte[] framed = enc.EncryptDataPageHeader(plain, rg, col, page);
            byte[] outBytes = dec.DecryptDataPageHeader(R(framed), rg, col, page);
            Assert.Equal(plain, outBytes);

            // wrong ordinal fails
            Assert.ThrowsAny<CryptographicException>(() =>
                dec.DecryptDataPageHeader(R(framed), rg, col, 1));
        }

        [Fact]
        public void Gcm_DataPageBody_Encrypt_RoundTrip() {
            var gcm = new AES_GCM_V1_Encryption {
                SecretKey = Key16,
                AadPrefix = Encoding.ASCII.GetBytes("writer-aad"),
                AadFileUnique = new byte[] { 0x10, 0x20, 0x30, 0x40 }
            };

            byte[] plain = Enumerable.Range(0, 257).Select(i => (byte)i).ToArray();
            short rg = 0, col = 0, page = 5;

            byte[] framed = gcm.EncryptDataPage(plain, rg, col, page);
            byte[] outBytes = gcm.DecryptDataPage(R(framed), rg, col, page);

            Assert.Equal(plain, outBytes);
        }



        [Fact]
        public void Ctr_DataPageBody_Encrypt_RoundTrip() {
            AES_GCM_CTR_V1_Encryption enc = MakeCtr(Key32);
            AES_GCM_CTR_V1_Encryption dec = MakeCtr(Key32);
            byte[] plain = Enumerable.Range(0, 999).Select(i => (byte)(i % 251)).ToArray();

            short rg = 1, col = 1, page = 9;
            byte[] framed = enc.EncryptDataPage(plain, rg, col, page); // CTR framing
            byte[] outBytes = dec.DecryptDataPage(R(framed), rg, col, page);
            Assert.Equal(plain, outBytes);
        }

        [Theory]
        [InlineData(ParquetModules.ColumnMetaData)]
        [InlineData(ParquetModules.ColumnIndex)]
        [InlineData(ParquetModules.OffsetIndex)]
        [InlineData(ParquetModules.BloomFilter_Header)]
        [InlineData(ParquetModules.BloomFilter_Bitset)]
        public void Gcm_Module_Encrypt_RoundTrip(ParquetModules module) {
            AES_GCM_V1_Encryption enc = MakeGcm(Key16);
            AES_GCM_V1_Encryption dec = MakeGcm(Key16);
            byte[] plain = Encoding.ASCII.GetBytes($"mod-{module}");
            short rg = 7, col = 4;

            byte[] framed = enc.EncryptModuleGcm(plain, module, rg, col);
            byte[] outBytes = module switch {
                ParquetModules.ColumnMetaData => dec.DecryptColumnMetaData(R(framed), rg, col),
                ParquetModules.ColumnIndex => dec.DecryptColumnIndex(R(framed), rg, col),
                ParquetModules.OffsetIndex => dec.DecryptOffsetIndex(R(framed), rg, col),
                ParquetModules.BloomFilter_Header => dec.BloomFilterHeader(R(framed), rg, col),
                ParquetModules.BloomFilter_Bitset => dec.BloomFilterBitset(R(framed), rg, col),
                _ => throw new NotSupportedException()
            };
            Assert.Equal(plain, outBytes);
        }
    }
}
