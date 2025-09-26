using System;
using System.IO;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet.Bloom {
    /// <summary>
    /// Utilities to write/read Bloom Filter header + bitset and hook
    /// them into ColumnMetaData.{BloomFilterOffset,BloomFilterLength}.
    /// </summary>
    internal static class BloomFilterIO {
        public static (long Offset, int Length) WriteToStream(
            Stream output,
            SplitBlockBloomFilter filter,
            ColumnMetaData columnMeta,
            Func<Stream, ThriftCompactProtocolWriter> writerFactory) {
            if(output == null)
                throw new ArgumentNullException("output");
            if(filter == null)
                throw new ArgumentNullException("filter");
            if(columnMeta == null)
                throw new ArgumentNullException("columnMeta");
            if(writerFactory == null)
                throw new ArgumentNullException("writerFactory");
            if(!output.CanWrite)
                throw new InvalidOperationException("Output stream not writable.");

            long offset = output.Position;

            var hdr = new BloomFilterHeader {
                NumBytes = filter.NumberOfBlocks * 32,
                Algorithm = new BloomFilterAlgorithm { BLOCK = new SplitBlockAlgorithm() },
                Hash = new BloomFilterHash { XXHASH = new XxHash() },
                Compression = new BloomFilterCompression { UNCOMPRESSED = new Uncompressed() }
            };

            ThriftCompactProtocolWriter proto = writerFactory(output);
            hdr.Write(proto);

            byte[] bitset = filter.ToByteArray();
            output.Write(bitset, 0, bitset.Length);

            int length = checked((int)(output.Position - offset));

            columnMeta.BloomFilterOffset = offset;
            columnMeta.BloomFilterLength = length;

            return (offset, length);
        }

        /// <summary>
        /// Reads a BloomFilterHeader and its bitset using ColumnMetaData.{BloomFilterOffset,BloomFilterLength}
        /// and reconstructs a <see cref="SplitBlockBloomFilter"/>.
        /// </summary>
        public static SplitBlockBloomFilter ReadFromStream(
            Stream input,
            ColumnMetaData columnMeta,
            Func<Stream, ThriftCompactProtocolReader> readerFactory) {
            if(input == null)
                throw new ArgumentNullException(nameof(input));
            if(columnMeta == null)
                throw new ArgumentNullException(nameof(columnMeta));
            if(readerFactory == null)
                throw new ArgumentNullException(nameof(readerFactory));
            if(!input.CanRead)
                throw new InvalidOperationException("Input stream not readable.");
            if(!input.CanSeek)
                throw new InvalidOperationException("Input stream must be seekable to read bloom filter.");
            if(!columnMeta.BloomFilterOffset.HasValue)
                throw new InvalidOperationException("ColumnMetaData does not contain BloomFilterOffset.");

            long offset = columnMeta.BloomFilterOffset.Value;
            input.Seek(offset, SeekOrigin.Begin);

            ThriftCompactProtocolReader proto = readerFactory(input);
            BloomFilterHeader hdr = BloomFilterHeader.Read(proto);

            // Validate header
            if(hdr == null)
                throw new InvalidDataException("Missing bloom filter header.");
            if(hdr.Algorithm?.BLOCK == null)
                throw new NotSupportedException("Unsupported bloom filter algorithm (only BLOCK is supported).");
            if(hdr.Hash?.XXHASH == null)
                throw new NotSupportedException("Unsupported bloom filter hash (only XXHASH is supported).");
            if(hdr.Compression?.UNCOMPRESSED == null)
                throw new NotSupportedException("Unsupported bloom filter compression (only UNCOMPRESSED is supported).");
            if(hdr.NumBytes <= 0 || (hdr.NumBytes % 32) != 0)
                throw new InvalidDataException("Invalid bloom filter header: NumBytes must be positive and a multiple of 32.");

            // Read raw bitset immediately following the header
            int numBytes = hdr.NumBytes;
            byte[] data = new byte[numBytes];
            int read = input.Read(data, 0, numBytes);
            if(read != numBytes)
                throw new EndOfStreamException("Could not read bloom filter bitset.");

            int blocks = numBytes / 32;
            return SplitBlockBloomFilter.FromByteArray(blocks, data);
        }
    }
}