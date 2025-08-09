using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Extensions;
using Parquet.File;

namespace Parquet {
    /// <summary>
    /// Base class for reader and writer
    /// </summary>
    public class ParquetActor {
#pragma warning disable IDE1006
        internal static readonly byte[] MagicBytes = Encoding.ASCII.GetBytes("PAR1");
#pragma warning restore IDE1006

        private readonly Stream _fileStream;

        internal ParquetActor(Stream? fileStream) =>
            _fileStream = fileStream ?? throw new ArgumentNullException(nameof(fileStream));

        /// <summary>
        /// Original stream to write or read
        /// </summary>
        protected Stream Stream => _fileStream;

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

            if(!MagicBytes.SequenceEqual(head) || !MagicBytes.SequenceEqual(tail))
                throw new IOException($"not a parquet file, head: {head.ToHexString()}, tail: {tail.ToHexString()}");
        }

        internal async ValueTask<Parquet.Meta.FileMetaData> ReadMetadataAsync(CancellationToken cancellationToken = default) {
            int footerLength = await GoBeforeFooterAsync();
            byte[] footerData = await _fileStream.ReadBytesExactlyAsync(footerLength);
            using var ms = new MemoryStream(footerData);
            return Parquet.Meta.FileMetaData.Read(new Meta.Proto.ThriftCompactProtocolReader(ms));
        }

        internal async ValueTask<int> GoBeforeFooterAsync() {
            //go to -4 bytes (PAR1) -4 bytes (footer length number)
            _fileStream.Seek(-8, SeekOrigin.End);
            int footerLength = await _fileStream.ReadInt32Async();

            //set just before footer starts
            _fileStream.Seek(-8 - footerLength, SeekOrigin.End);

            return footerLength;
        }
    }
}