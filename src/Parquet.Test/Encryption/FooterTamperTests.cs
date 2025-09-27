// src/Parquet.Test/Encryption/FooterTamperTests.cs
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class FooterTamperTests : TestBase {

        private static MemoryStream CopyToMemory(Stream s) {
            var ms = new MemoryStream();
            s.CopyTo(ms);
            ms.Position = 0;
            return ms;
        }

        private static MemoryStream TamperTailMagic(Stream source, string newMagic /* 4 chars */) {
            if(newMagic is null || newMagic.Length != 4)
                throw new ArgumentException("newMagic must be exactly 4 ASCII chars.", nameof(newMagic));

            using MemoryStream src = CopyToMemory(source);
            byte[] buf = src.ToArray();

            // last 4 bytes = tail magic (encrypted files use "PARE")
            byte[] magicBytes = Encoding.ASCII.GetBytes(newMagic);
            Buffer.BlockCopy(magicBytes, 0, buf, buf.Length - 4, 4);

            return new MemoryStream(buf, writable: false);
        }

        private static MemoryStream TamperFooterLength(Stream source, int newFooterLen) {
            using MemoryStream src = CopyToMemory(source);
            byte[] buf = src.ToArray();

            // File layout ends with: [footer_len (LE 4 bytes)][tail_magic (4 bytes)]
            // Overwrite footer_len with nonsense to force a read failure.
            byte[] len = BitConverter.GetBytes(newFooterLen);
            Buffer.BlockCopy(len, 0, buf, buf.Length - 8, 4);

            return new MemoryStream(buf, writable: false);
        }

        [Fact]
        public async Task TailMagic_Tampered_ValidateFile_Fails_And_Open_Fails() {
            using Stream s = OpenTestFile("encryption/enc_footer_only.parquet");
            using MemoryStream tampered = TamperTailMagic(s, "PAXX");

            // 1) Validation must fail on bad tail magic
            var actor = new ParquetActor(tampered);
            await Assert.ThrowsAsync<IOException>(() => actor.ValidateFileAsync());

            // 2) Reader creation should fail
            tampered.Position = 0;
            await Assert.ThrowsAnyAsync<Exception>(async () => {
                using ParquetReader _ = await ParquetReader.CreateAsync(tampered, new ParquetOptions {
                    EncryptionKey = "footerKey-16byte",
                    AADPrefix = null
                });
            });
        }

        [Theory]
        [InlineData(0)]            // impossible small
        [InlineData(int.MaxValue)] // absurdly large
        public async Task FooterLength_Tampered_Reader_Fails(int newLen) {
            using Stream s = OpenTestFile("encryption/enc_footer_only.parquet");
            using MemoryStream tampered = TamperFooterLength(s, newLen);

            await Assert.ThrowsAnyAsync<Exception>(async () => {
                using ParquetReader _ = await ParquetReader.CreateAsync(tampered, new ParquetOptions {
                    EncryptionKey = "footerKey-16byte",
                    AADPrefix = null
                });
            });
        }

        [Fact]
        public async Task HeadMagic_Tampered_ValidateFile_Fails_And_Open_Fails() {
            using Stream s = OpenTestFile("encryption/enc_footer_only.parquet");
            using MemoryStream ms = new MemoryStream();
            s.CopyTo(ms);
            byte[] buf = ms.ToArray();

            // first 4 bytes = head magic; encrypted files use "PARE"
            byte[] bad = Encoding.ASCII.GetBytes("PAXX");
            Buffer.BlockCopy(bad, 0, buf, 0, 4);

            using var tampered = new MemoryStream(buf, writable: false);

            var actor = new ParquetActor(tampered);
            await Assert.ThrowsAsync<IOException>(() => actor.ValidateFileAsync());

            tampered.Position = 0;
            await Assert.ThrowsAnyAsync<Exception>(async () => {
                using ParquetReader _ = await ParquetReader.CreateAsync(tampered, new ParquetOptions {
                    EncryptionKey = "footerKey-16byte",
                    AADPrefix = null
                });
            });
        }
    }
}
