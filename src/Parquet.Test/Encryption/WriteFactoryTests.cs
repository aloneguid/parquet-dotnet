using System;
using System.IO;
using System.Text;
using Parquet.Encryption;
using Parquet.Meta.Proto;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class WriteFactoryTests {
        private static ThriftCompactProtocolWriter W(Stream s) => new ThriftCompactProtocolWriter(s);

        private static MemoryStream BuildFooterRegion(
            Meta.FileCryptoMetaData cryptoMeta,
            byte[] framedEncryptedFooter) {
            var ms = new MemoryStream();
            // [FileCryptoMetaData][framed encrypted footer]
            cryptoMeta.Write(W(ms));
            ms.Write(framedEncryptedFooter, 0, framedEncryptedFooter.Length);
            ms.Position = 0;
            return ms;
        }

        [Fact]
        public void Factory_Gcm_PrefixStored_RoundTripFooter() {
            string key = Convert.ToBase64String(new byte[16] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
            byte[] prefix = Encoding.ASCII.GetBytes("stored-prefix");
            (EncryptionBase enc, Meta.FileCryptoMetaData? meta) = EncryptionBase.CreateEncryptorForWrite(
                encryptionOrSigningKey: key,
                aadPrefixBytes: prefix,
                supplyAadPrefix: false,   // store in file
                useCtrVariant: false
            );

            // some plaintext footer bytes (would be a serialized FileMetaData in writer)
            byte[] footerPlain = Encoding.ASCII.GetBytes("tiny-footer");

            // encrypt
            byte[] framed = enc.EncryptFooter(footerPlain);

            // build an in-memory "footer region": meta + framed footer
            using MemoryStream region = BuildFooterRegion(meta, framed);

            // decrypt via the read-side factory
            byte[] decrypted = EncryptionBase.DecryptFooter(TestCryptoUtils.R(region), key, aadPrefix: null, out EncryptionBase? decr);
            Assert.Equal(footerPlain, decrypted);
        }

        [Fact]
        public void Factory_Gcm_PrefixSupplied_RoundTripFooter() {
            string key = Convert.ToBase64String(new byte[32] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 });
            byte[] prefix = Encoding.ASCII.GetBytes("supply-me");

            (EncryptionBase enc, Meta.FileCryptoMetaData? meta) = EncryptionBase.CreateEncryptorForWrite(
                encryptionOrSigningKey: key,
                aadPrefixBytes: prefix,
                supplyAadPrefix: true,   // do NOT store in file
                useCtrVariant: false
            );

            byte[] footerPlain = Encoding.ASCII.GetBytes("footer-AAD-supply");
            byte[] framed = enc.EncryptFooter(footerPlain);

            using MemoryStream region = BuildFooterRegion(meta, framed);

            // must supply prefix on decrypt
            byte[] decrypted = EncryptionBase.DecryptFooter(TestCryptoUtils.R(region), key, aadPrefix: Encoding.ASCII.GetString(prefix), out _);
            Assert.Equal(footerPlain, decrypted);
        }

        [Fact]
        public void Factory_CtrVariant_Still_GcmFooter_RoundTrip() {
            string key = "sixteen-byte-key"; // 16 bytes raw UTF-8
            byte[] prefix = Encoding.ASCII.GetBytes("ctr-variant");

            (EncryptionBase enc, Meta.FileCryptoMetaData? meta) = EncryptionBase.CreateEncryptorForWrite(
                encryptionOrSigningKey: key,
                aadPrefixBytes: prefix,
                supplyAadPrefix: false,
                useCtrVariant: true   // CTR affects page bodies; footer remains GCM
            );

            byte[] footerPlain = Encoding.ASCII.GetBytes("footer-gcm-under-ctr");
            byte[] framed = enc.EncryptFooter(footerPlain);

            using MemoryStream region = BuildFooterRegion(meta, framed);

            byte[] decrypted = EncryptionBase.DecryptFooter(TestCryptoUtils.R(region), key, aadPrefix: null, out _);
            Assert.Equal(footerPlain, decrypted);
        }
    }
}
