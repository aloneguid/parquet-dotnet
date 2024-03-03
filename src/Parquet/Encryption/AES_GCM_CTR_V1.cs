using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using Parquet.Meta.Proto;

namespace Parquet.Encryption {
    internal class AES_GCM_CTR_V1_Encryption : AES_GCM_V1_Encryption {
        public AES_GCM_CTR_V1_Encryption() {

        }

        protected override byte[] Decrypt(ThriftCompactProtocolReader reader, Meta.ParquetModules module, short? rowGroupOrdinal = null, short? columnOrdinal = null, short? pageOrdinal = null) {
            byte[] encryptionBufferLengthBytes = reader.ReadBytesExactly(4).EnsureLittleEndian();
            int encryptionBufferLength = BitConverter.ToInt32(encryptionBufferLengthBytes, 0);
            byte[] nonce = reader.ReadBytesExactly(12);
            int cipherTextLength = encryptionBufferLength - nonce.Length;
            byte[] cipherText = reader.ReadBytesExactly(cipherTextLength);

            using var cipherStream = new MemoryStream(cipherText);
            using var plaintextStream = new MemoryStream(); //This will contain the decrypted result
            AesCtrTransform(DecryptionKey!, nonce, cipherStream, plaintextStream);

            plaintextStream.Position = 0;
            return plaintextStream.ToArray();
        }

        // TODO: This hasn't been tested!!!
        // Source: https://stackoverflow.com/a/51188472/1458738
        private static void AesCtrTransform(byte[] key, byte[] salt, Stream inputStream, Stream outputStream) {
            using var aes = Aes.Create();
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.None;

            int blockSize = aes.BlockSize / 8;
            if(salt.Length != blockSize) {
                throw new ArgumentException(
                    "Salt size must be same as block size " +
                    $"(actual: {salt.Length}, expected: {blockSize})");
            }

            byte[] counter = (byte[])salt.Clone();

            var xorMask = new Queue<byte>();

            byte[] zeroIv = new byte[blockSize];
            ICryptoTransform counterEncryptor = aes.CreateEncryptor(key, zeroIv);

            int b;
            while((b = inputStream.ReadByte()) != -1) {
                if(xorMask.Count == 0) {
                    byte[] counterModeBlock = new byte[blockSize];

                    counterEncryptor.TransformBlock(
                        counter, 0, counter.Length, counterModeBlock, 0);

                    for(int i2 = counter.Length - 1; i2 >= 0; i2--) {
                        if(++counter[i2] != 0) {
                            break;
                        }
                    }

                    foreach(byte b2 in counterModeBlock) {
                        xorMask.Enqueue(b2);
                    }
                }

                byte mask = xorMask.Dequeue();
                outputStream.WriteByte((byte)(((byte)b) ^ mask));
            }
        }
    }
}
