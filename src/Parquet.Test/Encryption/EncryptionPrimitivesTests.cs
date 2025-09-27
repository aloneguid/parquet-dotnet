using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Parquet.Encryption;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Encryption {
    public class EncryptionPrimitivesTests {
        // --- Shared fixtures ---
        private static readonly byte[] Key128 = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        private static readonly byte[] Key256 = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
        private static readonly byte[] AadPrefix = Encoding.ASCII.GetBytes("employees_2025-09-27.part0");
        private static readonly byte[] AadFileUnique = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD }; // short is fine—spec allows impl-defined length
        private const short RowGroup = 3;
        private const short Column = 2;
        private const short Page = 7;

        private static byte[] Le16(short v) => BitConverter.GetBytes(v);

        private static byte[] BuildAadSuffix(ParquetModules module, bool addRowCol, bool addPage) {
            // AAD suffix = fileUnique || moduleType(1B) || [rowGroup(LE16)] || [column(LE16)] || [page(LE16)]
            using var ms = new MemoryStream();
            ms.Write(AadFileUnique, 0, AadFileUnique.Length);
            ms.WriteByte((byte)module);
            if(addRowCol) {
                ms.Write(Le16(RowGroup), 0, 2);
                ms.Write(Le16(Column), 0, 2);
            }
            if(addPage) {
                ms.Write(Le16(Page), 0, 2);
            }
            return ms.ToArray();
        }

        private static ThriftCompactProtocolReader MakeReader(byte[] buffer) {
            var ms = new MemoryStream(buffer);
            return new ThriftCompactProtocolReader(ms);
        }

        private static byte[] FrameGcmBuffer(byte[] nonce12, byte[] ciphertext, byte[] tag16) {
            // length = nonce + ciphertext + tag
            int len = nonce12.Length + ciphertext.Length + tag16.Length;
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(len), 0, 4);              // 4B little-endian length
            ms.Write(nonce12, 0, nonce12.Length);                    // 12B nonce
            ms.Write(ciphertext, 0, ciphertext.Length);              // ciphertext
            ms.Write(tag16, 0, tag16.Length);                        // 16B tag
            return ms.ToArray();
        }

        private static byte[] FrameCtrBuffer(byte[] nonce12, byte[] ciphertext) {
            int len = nonce12.Length + ciphertext.Length;
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(len), 0, 4);
            ms.Write(nonce12, 0, nonce12.Length);
            ms.Write(ciphertext, 0, ciphertext.Length);
            return ms.ToArray();
        }

        // --- AES_GCM_V1 ---

        [Fact]
        public void AES_GCM_V1_RoundTrip_DataPage_WithAAD_Succeeds() {
            // Arrange
            byte[] plaintext = Encoding.ASCII.GetBytes("hello world, this is a data page payload!");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            byte[] aad = AadPrefix.Concat(BuildAadSuffix(ParquetModules.Data_Page, addRowCol: true, addPage: true)).ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key256, 16);
#else
            using var gcm = new AesGcm(Key256);
#endif
            byte[] ciphertext = new byte[plaintext.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);

            byte[] framed = FrameGcmBuffer(nonce, ciphertext, tag);
            ThriftCompactProtocolReader reader = MakeReader(framed);

            var enc = new AES_GCM_V1_Encryption {
                DecryptionKey = Key256,
                AadPrefix = AadPrefix,
                AadFileUnique = AadFileUnique
            };

            // Act
            byte[] result = enc.DecryptDataPage(reader, RowGroup, Column, Page);

            // Assert
            Assert.Equal(plaintext, result);
        }

        [Fact]
        public void AES_GCM_V1_WrongAadPrefix_Throws() {
            // Arrange
            byte[] plaintext = Encoding.ASCII.GetBytes("column index payload");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            byte[] correctAad = AadPrefix.Concat(BuildAadSuffix(ParquetModules.ColumnIndex, addRowCol: true, addPage: false)).ToArray();

#if NET8_0_OR_GREATER
            using var gcm = new AesGcm(Key128, 16);
#else
            using var gcm = new AesGcm(Key128);
#endif
            byte[] ciphertext = new byte[plaintext.Length];
            byte[] tag = new byte[16];
            gcm.Encrypt(nonce, plaintext, ciphertext, tag, correctAad);

            byte[] framed = FrameGcmBuffer(nonce, ciphertext, tag);
            ThriftCompactProtocolReader reader = MakeReader(framed);

            byte[] wrongPrefix = Encoding.ASCII.GetBytes("WRONG_PREFIX");
            var enc = new AES_GCM_V1_Encryption {
                DecryptionKey = Key128,
                AadPrefix = wrongPrefix,       // <-- wrong on purpose
                AadFileUnique = AadFileUnique
            };

            // Act + Assert
            Assert.ThrowsAny<CryptographicException>(() => {
                enc.DecryptColumnIndex(reader, RowGroup, Column);
            });
        }

        // --- AES_GCM_CTR_V1 (pages only use CTR) ---
        // NOTE: This test describes the expected behavior per spec.
        // It will FAIL with the current implementation because the IV is 16 bytes,
        // not 12, and must be nonce(12) || counter(4) with first 31 bits 0 and last bit 1.

        [Fact]
        public void AES_GCM_CTR_V1_RoundTrip_DataPage_Uses16ByteIV_Succeeds_AfterFix() {
            // Arrange
            byte[] plaintext = Encoding.ASCII.GetBytes("fast CTR page data payload");
            byte[] nonce = RandomNumberGenerator.GetBytes(12);

            // Build 16-byte IV = nonce(12) || counter(4) where counter = 0x00000001 (big-endian)
            byte[] iv16 = new byte[16];
            Buffer.BlockCopy(nonce, 0, iv16, 0, 12);
            iv16[12] = 0x00;
            iv16[13] = 0x00;
            iv16[14] = 0x00;
            iv16[15] = 0x01; // last bit set

            // Encrypt with AES-CTR (ECB keystream on IV, increment counter big-endian)
            byte[] ciphertext;
            using(var aes = Aes.Create()) {
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.None;
                using ICryptoTransform encryptor = aes.CreateEncryptor(Key256, new byte[16]); // IV not used in ECB
                ciphertext = XorWithCtrKeystream(encryptor, iv16, plaintext);
            }

            byte[] framed = FrameCtrBuffer(nonce, ciphertext);
            ThriftCompactProtocolReader reader = MakeReader(framed);

            var enc = new AES_GCM_CTR_V1_Encryption {
                DecryptionKey = Key256,
                AadPrefix = AadPrefix,       // not used by CTR pages, but harmless
                AadFileUnique = AadFileUnique
            };

            // Act
            byte[] result = enc.DecryptDataPage(reader, RowGroup, Column, Page);

            // Assert
            Assert.Equal(plaintext, result);
        }

        // --- Magic bytes detection ---

        [Fact]
        public async Task ValidateFileAsync_SetsIsEncryptedFile_ForPARE() {
            // Arrange:  PARE .... PARE
            using var ms = new MemoryStream();
            ms.Write(Encoding.ASCII.GetBytes("PARE"), 0, 4);              // head
            ms.Write(new byte[] { 0, 1, 2, 3, 4, 5 }, 0, 6);              // body filler
            ms.Write(Encoding.ASCII.GetBytes("PARE"), 0, 4);              // tail
            ms.Position = 0;

            var actor = new TestParquetActor(ms);

            // Act
            await actor.CallValidateFileAsync();

            // Assert
            Assert.True(actor.IsEncryptedFile);
        }

        // --- Helpers ---

        // Re-usable CTR keystream xorer for test encryption
        private static byte[] XorWithCtrKeystream(ICryptoTransform ecbEncryptor, byte[] iv16, byte[] input) {
            int blockSize = 16;
            byte[] counter = (byte[])iv16.Clone();
            byte[] output = new byte[input.Length];

            int i = 0;
            while(i < input.Length) {
                byte[] ksBlock = new byte[blockSize];
                ecbEncryptor.TransformBlock(counter, 0, blockSize, ksBlock, 0);

                int n = Math.Min(blockSize, input.Length - i);
                for(int j = 0; j < n; j++) {
                    output[i + j] = (byte)(input[i + j] ^ ksBlock[j]);
                }

                // Increment counter big-endian in the last 4 bytes
                for(int p = 15; p >= 12; p--) {
                    if(++counter[p] != 0)
                        break;
                }

                i += n;
            }

            return output;
        }

        // Expose ValidateFileAsync via a tiny test shim
        private sealed class TestParquetActor : ParquetActor {
            public TestParquetActor(Stream s) : base(s) { }
            public new bool IsEncryptedFile => base.IsEncryptedFile;
            public Task CallValidateFileAsync() => base.ValidateFileAsync();
        }
    }
}
