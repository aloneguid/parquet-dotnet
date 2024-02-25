using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Encryption;
using Parquet.Extensions;

namespace Parquet {
    /// <summary>
    /// Base class for reader and writer
    /// </summary>
    public class ParquetActor {
#pragma warning disable IDE1006
        internal static readonly byte[] MagicBytes = Encoding.ASCII.GetBytes("PAR1");
        internal static readonly byte[] MagicBytesEncrypted = Encoding.ASCII.GetBytes("PARE");
#pragma warning restore IDE1006

        private readonly Stream _fileStream;
        private BinaryWriter? _binaryWriter;

        internal ParquetActor(Stream? fileStream) =>
            _fileStream = fileStream ?? throw new ArgumentNullException(nameof(fileStream));

        /// <summary>
        /// Original stream to write or read
        /// </summary>
        protected Stream Stream => _fileStream;

        internal bool IsEncryptedFile;

        internal BinaryWriter Writer => _binaryWriter ??= new BinaryWriter(_fileStream);

        /// <summary>
        /// Validates that this file is a valid parquet file by reading head and tail of it
        /// </summary>
        /// <returns></returns>
        /// <exception cref="IOException"></exception>
        protected async Task ValidateFileAsync() {
            _fileStream.Seek(0, SeekOrigin.Begin);
            byte[] head = await _fileStream.ReadBytesExactlyAsync(4);

            _fileStream.Seek(-4, SeekOrigin.End);
            byte[] tail = await _fileStream.ReadBytesExactlyAsync(4);

            if(!MagicBytes.SequenceEqual(head) || !MagicBytes.SequenceEqual(tail)) {
                if(!MagicBytesEncrypted.SequenceEqual(head) || !MagicBytesEncrypted.SequenceEqual(tail)) {
                    throw new IOException($"not a parquet file, head: {head.ToHexString()}, tail: {tail.ToHexString()}");
                }
                IsEncryptedFile = true;
            }
        }

        internal async ValueTask<Meta.FileMetaData> ReadMetadataAsync(string? decryptionKey = null, string? aadPrefix = null, CancellationToken cancellationToken = default) {
            int footerLength = await GoBeforeFooterAsync();
            byte[] footerData = await _fileStream.ReadBytesExactlyAsync(footerLength);
            using var ms = new MemoryStream(footerData);

            EncryptionBase? decrypter = null;
            var protoReader = new Meta.Proto.ThriftCompactProtocolReader(ms);
            if(IsEncryptedFile) {
                byte[] decryptedFooter = EncryptionBase.DecryptFooter(protoReader, decryptionKey!, aadPrefix, out decrypter);

                //re-use the same proto reader
                ms.SetLength(0);
                ms.Write(decryptedFooter, 0, decryptedFooter.Length);
                ms.Position = 0;
            }

            var fileMetaData = Meta.FileMetaData.Read(protoReader);
            fileMetaData.Decrypter = decrypter; //save the decrypter because we will need it to decrypt every module

            return fileMetaData;
        }

        internal async ValueTask<int> GoBeforeFooterAsync() {
            //go to -4 bytes (PAR1) -4 bytes (footer length number)
            _fileStream.Seek(-8, SeekOrigin.End);
            int footerLength = await _fileStream.ReadInt32Async();

            //set just before footer starts
            _fileStream.Seek(-8 - footerLength, SeekOrigin.End);

            return footerLength;
        }

        //Source: https://stackoverflow.com/a/51188472/1458738
        private static void AesCtrTransform(byte[] key, byte[] salt, Stream inputStream, Stream outputStream) {
#pragma warning disable SYSLIB0021 // Type or member is obsolete (TODO: replace this with non-obsolete implementation)
            using var aes = new AesManaged { Mode = CipherMode.ECB, Padding = PaddingMode.None };
#pragma warning restore SYSLIB0021 // Type or member is obsolete

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