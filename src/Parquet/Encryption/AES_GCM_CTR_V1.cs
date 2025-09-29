using System;
using System.IO;
using System.Security.Cryptography;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
    internal class AES_GCM_CTR_V1_Encryption : AES_GCM_V1_Encryption {
        private const int NonceLength = 12;

        public AES_GCM_CTR_V1_Encryption() {
        }

        protected override byte[] Decrypt(
            ThriftCompactProtocolReader reader,
            Meta.ParquetModules module,
            short? rowGroupOrdinal = null,
            short? columnOrdinal = null,
            short? pageOrdinal = null
        ) {
            // Data/Dictionary page bodies use CTR; everything else falls back to base (GCM)
            if(module == Meta.ParquetModules.Data_Page || module == Meta.ParquetModules.Dictionary_Page) {
                // Spec framing: length(4 LE) | nonce(12) | ciphertext
                byte[] lenBytes = ReadExactlyOrInvalid(reader, 4, "CTR length").EnsureLittleEndian();
                int totalLength = BitConverter.ToInt32(lenBytes, 0);

                const int NonceLen = 12;
                if(totalLength < NonceLen)
                    throw new InvalidDataException($"Encrypted CTR buffer too small ({totalLength} bytes).");

                byte[] nonce = ReadExactlyOrInvalid(reader, NonceLen, "CTR nonce");
                int ctLen = totalLength - NonceLen;
                if(ctLen < 0)
                    throw new InvalidDataException("Invalid ciphertext length for AES-CTR framing.");

                byte[] ct = ReadExactlyOrInvalid(reader, ctLen, "CTR ciphertext");
                EncTrace.FrameCtr("Decrypt", module, totalLength, nonce, ctLen, rowGroupOrdinal, columnOrdinal, pageOrdinal);


                // IV = nonce(12) || 0x00000001 (big-endian)
                byte[] iv16 = new byte[16];
                Buffer.BlockCopy(nonce, 0, iv16, 0, NonceLen);
                iv16[12] = 0x00;
                iv16[13] = 0x00;
                iv16[14] = 0x00;
                iv16[15] = 0x01;

                using var cipherStream = new MemoryStream(ct, writable: false);
                using var plainStream = new MemoryStream(ctLen);
                AesCtrTransform(FooterEncryptionKey!, iv16, cipherStream, plainStream);

                return plainStream.ToArray();
            }

            // Non-page modules remain GCM-framed (length|nonce|ciphertext|tag)
            return base.Decrypt(reader, module, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        }

        private static byte[] EncryptCtrPageBody(byte[] body, byte[] key) {
            if(key == null || (key.Length != 16 && key.Length != 24 && key.Length != 32))
                throw new InvalidDataException("Missing or invalid AES key for AES-GCM-CTR-V1 encryption.");

            // nonce(12) for framing; IV = nonce(12) || 00 00 00 01 (counter starts at 1)
            byte[] nonce12 = new byte[NonceLength];
#if NET8_0_OR_GREATER
    RandomNumberGenerator.Fill(nonce12);
#else
            using(var rng = RandomNumberGenerator.Create())
                rng.GetBytes(nonce12);
#endif

            byte[] iv16 = new byte[16];
            Buffer.BlockCopy(nonce12, 0, iv16, 0, NonceLength);
            iv16[12] = 0x00;
            iv16[13] = 0x00;
            iv16[14] = 0x00;
            iv16[15] = 0x01; // big-endian counter starts at 1

            // Optional but recommended: enforce per-key page count limit
            // (call the instance method if this lives inside your EncryptionBase)
            // CountPageEncryption();

            using var inMs = new MemoryStream(body, writable: false);
            using var outMs = new MemoryStream(body.Length);
            AesCtrTransform(key, iv16, inMs, outMs);  // must increment last 4 bytes as big-endian counter
            byte[] ct = outMs.ToArray();

            // CTR framing: length(4 LE) | nonce(12) | ciphertext
            int payloadLen = NonceLength + ct.Length;

            byte[] framed = new byte[4 + payloadLen];

            // Always emit LE length, regardless of platform endianness
            byte[] lenLE = BitConverter.GetBytes(payloadLen);
            if(!BitConverter.IsLittleEndian)
                Array.Reverse(lenLE);
            Buffer.BlockCopy(lenLE, 0, framed, 0, 4);

            Buffer.BlockCopy(nonce12, 0, framed, 4, NonceLength);
            Buffer.BlockCopy(ct, 0, framed, 4 + NonceLength, ct.Length);
            return framed;
        }


        public override byte[] EncryptDataPage(
            byte[] body,
            short rowGroupOrdinal,
            short columnOrdinal,
            short pageOrdinal
        ) {
            CountPageEncryption();
            return EncryptCtrPageBody(body, FooterEncryptionKey!);
        }

        public override byte[] EncryptDictionaryPage(
            byte[] body,
            short rowGroupOrdinal,
            short columnOrdinal
        ) {
            CountPageEncryption();
            return EncryptCtrPageBody(body, FooterEncryptionKey!);
        }

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
