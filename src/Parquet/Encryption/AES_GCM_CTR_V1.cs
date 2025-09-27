using System;
using System.IO;
using System.Security.Cryptography;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
    internal class AES_GCM_CTR_V1_Encryption : AES_GCM_V1_Encryption {
        public AES_GCM_CTR_V1_Encryption() {
        }

        protected override byte[] Decrypt(
            ThriftCompactProtocolReader reader,
            Meta.ParquetModules module,
            short? rowGroupOrdinal = null,
            short? columnOrdinal = null,
            short? pageOrdinal = null) {

            // Everything else (page headers, indexes, bloom filters, column metadata, footer) remains AES-GCM.
            if(module == Meta.ParquetModules.Data_Page || module == Meta.ParquetModules.Dictionary_Page) {
                // CTR framing: length(4 LE) | nonce(12) | ciphertext
                byte[] lengthBytes = ReadExactlyOrInvalid(reader, 4, "CTR length").EnsureLittleEndian();
                int totalLength = BitConverter.ToInt32(lengthBytes, 0);

                // NEW: validate total length before any reads
                const int NonceLen = 12;
                if(totalLength < NonceLen) {
                    throw new InvalidDataException("Encrypted CTR buffer too small: missing 12-byte nonce.");
                }

                byte[] nonce12 = ReadExactlyOrInvalid(reader, 12, "CTR nonce");
                int cipherTextLength = totalLength - nonce12.Length;
                if(cipherTextLength < 0) {
                    throw new InvalidDataException("Invalid AES-GCM-CTR module length.");
                }

                byte[] cipherText = ReadExactlyOrInvalid(reader, cipherTextLength, "CTR ciphertext");

                // Build 16-byte IV = nonce(12) || 0x00000001 (big-endian).
                byte[] iv16 = new byte[16];
                Buffer.BlockCopy(nonce12, 0, iv16, 0, 12);
                iv16[12] = 0x00;
                iv16[13] = 0x00;
                iv16[14] = 0x00;
                iv16[15] = 0x01;

                using var cipherStream = new MemoryStream(cipherText, writable: false);
                using var plainStream = new MemoryStream(cipherText.Length);

                AesCtrTransform(SecretKey!, iv16, cipherStream, plainStream);

                return plainStream.ToArray();
            }

            // Delegate to GCM for all non-page modules (keeps correct framing + AAD handling).
            return base.Decrypt(reader, module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        }

        private static byte[] EncryptCtrPageBody(byte[] body, byte[] key) {
            if(key == null || (key.Length != 16 && key.Length != 24 && key.Length != 32))
                throw new InvalidDataException("Missing or invalid AES key for AES-GCM-CTR-V1 encryption.");

            // nonce(12) for framing; IV = nonce(12) || 00 00 00 01 (counter starts at 1)
            byte[] nonce12 = new byte[12];
#if NET8_0_OR_GREATER
    RandomNumberGenerator.Fill(nonce12);
#else
            using(var rng = RandomNumberGenerator.Create())
                rng.GetBytes(nonce12);
#endif
            byte[] iv16 = new byte[16];
            Buffer.BlockCopy(nonce12, 0, iv16, 0, 12);
            iv16[12] = 0;
            iv16[13] = 0;
            iv16[14] = 0;
            iv16[15] = 1;

            using var inMs = new MemoryStream(body, writable: false);
            using var outMs = new MemoryStream(body.Length);
            AesCtrTransform(key, iv16, inMs, outMs);
            byte[] ct = outMs.ToArray();

            // CTR framing: length(4 LE) | nonce(12) | ciphertext
            int len = 12 + ct.Length;
            byte[] framed = new byte[4 + len];
            BitConverter.GetBytes(len).CopyTo(framed, 0);
            Buffer.BlockCopy(nonce12, 0, framed, 4, 12);
            Buffer.BlockCopy(ct, 0, framed, 4 + 12, ct.Length);
            return framed;
        }

        public override byte[] EncryptDataPage(byte[] body, short rowGroupOrdinal, short columnOrdinal, short pageOrdinal)
            => EncryptCtrPageBody(body, SecretKey!);

        public override byte[] EncryptDictionaryPage(byte[] body, short rowGroupOrdinal, short columnOrdinal)
            => EncryptCtrPageBody(body, SecretKey!);

        private static void AesCtrTransform(byte[] key, byte[] iv16, Stream inputStream, Stream outputStream) {
            if(key == null)
                throw new ArgumentNullException(nameof(key));
            if(iv16 == null)
                throw new ArgumentNullException(nameof(iv16));
            if(iv16.Length != 16)
                throw new ArgumentException("IV must be 16 bytes for AES-CTR.", nameof(iv16));
            if(inputStream == null)
                throw new ArgumentNullException(nameof(inputStream));
            if(outputStream == null)
                throw new ArgumentNullException(nameof(outputStream));

            using var aes = Aes.Create();
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.None;
            aes.Key = key;

            const int blockSize = 16;
            byte[] counter = (byte[])iv16.Clone();
            byte[] inBuf = new byte[blockSize];
            byte[] ksBlock = new byte[blockSize];

            using ICryptoTransform ecbEncryptor = aes.CreateEncryptor(aes.Key, new byte[blockSize]); // IV unused for ECB

            int read;
            while((read = inputStream.Read(inBuf, 0, blockSize)) > 0) {
                // Generate keystream for current counter
                ecbEncryptor.TransformBlock(counter, 0, blockSize, ksBlock, 0);

                // XOR (partial for last chunk)
                for(int i = 0; i < read; i++) {
                    inBuf[i] = (byte)(inBuf[i] ^ ksBlock[i]);
                }
                outputStream.Write(inBuf, 0, read);

                // Increment only the last 4 bytes (big-endian)
                for(int p = 15; p >= 12; p--) {
                    if(++counter[p] != 0)
                        break;
                }
                // overflow check (should never happen in practice, as it would require >2^32 blocks = >64TB per page)
                bool wrapped = true;
                for(int p = 12; p <= 15; p++) {
                    if(counter[p] != 0) { wrapped = false; break; }
                }
                if(wrapped) {
                    throw new CryptographicException("AES-CTR counter overflow (exceeded 2^32-1 blocks).");
                }
            }
        }
    }
}
