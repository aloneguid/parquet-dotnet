// src/Parquet.Test/Encryption/PageOrdinal_Reset_Tests.cs
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
    public class AadOrdinalTests {
        private static readonly byte[] Key = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        private static readonly byte[] Prefix = Encoding.ASCII.GetBytes("ord-reset");
        private static readonly byte[] Unique = new byte[] { 1, 3, 3, 7 };
        private static byte[] EncryptHeader(short rg, short col, short pageOrd, string txt) {
            byte[] plain = Encoding.ASCII.GetBytes(txt);
            byte[] nonce = RandomNumberGenerator.GetBytes(12);
            byte[] tag = new byte[16];
            byte[] ct = new byte[plain.Length];

            byte[] aad = Prefix
                .Concat(Unique)
                .Concat(new byte[] { (byte)ParquetModules.Data_PageHeader })
                .Concat(BitConverter.GetBytes(rg))
                .Concat(BitConverter.GetBytes(col))
                .Concat(BitConverter.GetBytes(pageOrd))
                .ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key, 16);
#else
            using var gcm = new AesGcm(Key);
#endif
            gcm.Encrypt(nonce, plain, ct, tag, aad);
            return TestCryptoUtils.FrameGcm(nonce, ct, tag);
        }

        [Fact]
        public void Ordinal_Resets_Per_Column_And_RowGroup() {
            var dec = new AES_GCM_V1_Encryption { SecretKey = Key, AadPrefix = Prefix, AadFileUnique = Unique };

            // RG0 COL0 page0
            byte[] rg0c0p0 = EncryptHeader(0, 0, 0, "rg0c0p0");
            Assert.Equal(Encoding.ASCII.GetBytes("rg0c0p0"),
                dec.DecryptDataPageHeader(R(rg0c0p0), 0, 0, 0));

            // RG0 COL0 page1
            byte[] rg0c0p1 = EncryptHeader(0, 0, 1, "rg0c0p1");
            Assert.Equal(Encoding.ASCII.GetBytes("rg0c0p1"),
                dec.DecryptDataPageHeader(R(rg0c0p1), 0, 0, 1));

            // RG0 COL1 page0 (ordinal resets to 0 for a new column)
            byte[] rg0c1p0 = EncryptHeader(0, 1, 0, "rg0c1p0");
            Assert.Equal(Encoding.ASCII.GetBytes("rg0c1p0"),
                dec.DecryptDataPageHeader(R(rg0c1p0), 0, 1, 0));

            // RG1 COL0 page0 (ordinal resets to 0 for a new row group)
            byte[] rg1c0p0 = EncryptHeader(1, 0, 0, "rg1c0p0");
            Assert.Equal(Encoding.ASCII.GetBytes("rg1c0p0"),
                dec.DecryptDataPageHeader(R(rg1c0p0), 1, 0, 0));

            // Negative checks: wrong ordinals/rg/col must fail
            Assert.ThrowsAny<CryptographicException>(() =>
                dec.DecryptDataPageHeader(R(rg0c0p0), 0, 0, 1));
            Assert.ThrowsAny<CryptographicException>(() =>
                dec.DecryptDataPageHeader(R(rg0c1p0), 0, 0, 0));
            Assert.ThrowsAny<CryptographicException>(() =>
                dec.DecryptDataPageHeader(R(rg1c0p0), 0, 0, 0));
        }

        private static byte[] Frame(byte[] n, byte[] c, byte[] t) {
            int len = n.Length + c.Length + t.Length;
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(len), 0, 4);
            ms.Write(n, 0, n.Length);
            ms.Write(c, 0, c.Length);
            ms.Write(t, 0, t.Length);
            return ms.ToArray();
        }
        private static ThriftCompactProtocolReader R(byte[] buf) => new ThriftCompactProtocolReader(new MemoryStream(buf));

        [Fact]
        public void ColumnOrdinal_Is_Enforced() {
            byte[] plain = Encoding.ASCII.GetBytes("hdr");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);
            byte[] tag = new byte[16];
            byte[] ct = new byte[plain.Length];

            // Encrypt for column 0
            byte[] aad = Prefix
                .Concat(Unique)
                .Concat(new byte[] { (byte)ParquetModules.Data_PageHeader })
                .Concat(BitConverter.GetBytes((short)0)) // rg
                .Concat(BitConverter.GetBytes((short)0)) // col 0
                .Concat(BitConverter.GetBytes((short)0)) // page 0
                .ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key, 16);
#else
            using var gcm = new AesGcm(Key);
#endif
            gcm.Encrypt(nonce, plain, ct, tag, aad);
            byte[] framed = Frame(nonce, ct, tag);

            var dec = new AES_GCM_V1_Encryption { SecretKey = Key, AadPrefix = Prefix, AadFileUnique = Unique };

            // Correct column works
            Assert.Equal(plain, dec.DecryptDataPageHeader(R(framed), 0, 0, 0));

            // Wrong column fails
            Assert.ThrowsAny<CryptographicException>(() =>
                dec.DecryptDataPageHeader(R(framed), 0, 1, 0));
        }
    }
}
