using System;

namespace Parquet.Bloom {
    /// <summary>
    /// Split-Block Bloom Filter per Parquet spec.
    /// - z blocks; each block is 8 x 32-bit words = 256 bits.
    /// - Block selection uses the high 32 bits of the 64-bit hash (multiply-shift).
    /// - Bit selection in each 32-bit word uses the low 32 bits of the hash with 8 salts.
    /// </summary>
    public sealed class SplitBlockBloomFilter {
        // Fixed salts (unsigned 32-bit).
        /// <summary>
        /// Fixed salts used for bit selection in each 32-bit word of the block.
        /// </summary>
        public static readonly uint[] Salts = new uint[8]
        {
            0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
            0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U
        };

        private readonly uint[] _words;

        /// <summary>
        /// Gets the number of blocks in the Bloom filter.
        /// </summary>
        public int NumberOfBlocks { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SplitBlockBloomFilter"/> class with the specified number of blocks.
        /// </summary>
        /// <param name="numberOfBlocks">The number of blocks in the Bloom filter. Must be greater than or equal to 1.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="numberOfBlocks"/> is less than or equal to 0.</exception>
        public SplitBlockBloomFilter(int numberOfBlocks) {
            if(numberOfBlocks <= 0) {
                throw new ArgumentOutOfRangeException("numberOfBlocks", "numberOfBlocks must be >= 1.");
            }

            this.NumberOfBlocks = numberOfBlocks;
            this._words = new uint[numberOfBlocks * 8];
        }

        /// <summary>
        /// Insert a value given as a byte array. The bytes are hashed with XXH64 (seed = 0),
        /// then mapped to a block and per-word bit positions per the Split-Block algorithm.
        /// </summary>
        public void Insert(byte[] data) {
            if(data == null) {
                throw new ArgumentNullException("data");
            }
            ulong h = BloomHasher.HashPlainEncoded(data);
            this.Insert(h);
        }

        /// <summary>
        /// Insert a slice of a byte array (offset/length). The slice is hashed with XXH64 (seed = 0).
        /// </summary>
        public void Insert(byte[] data, int offset, int length) {
            if(data == null) {
                throw new ArgumentNullException("data");
            }
            if(offset < 0 || length < 0 || offset + length > data.Length) {
                throw new ArgumentOutOfRangeException("offset/length");
            }

            ulong h = BloomHasher.HashPlainEncoded(data, offset, length);
            this.Insert(h);
        }

        /// <summary>
        /// Checks membership for a byte array by hashing it with XXH64 (seed = 0) and probing the filter.
        /// </summary>
        public bool MightContain(byte[] data) {
            if(data == null) {
                throw new ArgumentNullException("data");
            }
            ulong h = BloomHasher.HashPlainEncoded(data);
            return this.MightContain(h);
        }

        /// <summary>
        /// Checks membership for a slice of a byte array (offset/length), hashing with XXH64 (seed = 0).
        /// </summary>
        public bool MightContain(byte[] data, int offset, int length) {
            if(data == null) {
                throw new ArgumentNullException("data");
            }
            if(offset < 0 || length < 0 || offset + length > data.Length) {
                throw new ArgumentOutOfRangeException("offset/length");
            }

            ulong h = BloomHasher.HashPlainEncoded(data, offset, length);
            return this.MightContain(h);
        }

        /// <summary>
        /// Insert a 64-bit hash value.
        /// High 32 bits choose the block; low 32 bits choose one bit in each 32-bit word of the block.
        /// </summary>
        public void Insert(ulong hash64) {
            int blockIndex = SplitBlockBloomFilter.MapHashToBlock(hash64, this.NumberOfBlocks);
            uint x = (uint)(hash64 & 0xFFFFFFFFUL);
            this.BlockInsert(blockIndex, x);
        }

        /// <summary>
        /// Membership check for a 64-bit hash value.
        /// </summary>
        public bool MightContain(ulong hash64) {
            int blockIndex = SplitBlockBloomFilter.MapHashToBlock(hash64, this.NumberOfBlocks);
            uint x = (uint)(hash64 & 0xFFFFFFFFUL);
            return this.BlockCheck(blockIndex, x);
        }

        /// <summary>
        /// Map 64-bit hash to a block index using multiply-shift: ((h >> 32) * z) >> 32
        /// </summary>
        public static int MapHashToBlock(ulong hash64, int numberOfBlocks) {
            if(numberOfBlocks <= 0) {
                throw new ArgumentOutOfRangeException("numberOfBlocks", "numberOfBlocks must be >= 1.");
            }

            ulong high = hash64 >> 32;
            ulong product = high * (ulong)numberOfBlocks;
            int index = (int)(product >> 32);
            return index;
        }

        /// <summary>
        /// Returns how many bytes are needed for the given number of blocks.
        /// </summary>
        public static int BytesForBlocks(int numberOfBlocks) {
            if(numberOfBlocks <= 0) {
                throw new ArgumentOutOfRangeException("numberOfBlocks", "numberOfBlocks must be >= 1.");
            }

            return numberOfBlocks * 32;
        }

        /// <summary>
        /// Serialize the bitset to a little-endian byte array (8 uints per block).
        /// </summary>
        public byte[] ToByteArray() {
            int len = this._words.Length;
            byte[] bytes = new byte[len * 4];

            int o = 0;
            for(int i = 0; i < len; i++) {
                uint w = this._words[i];
                // Little-endian layout:
                bytes[o + 0] = (byte)(w & 0xFF);
                bytes[o + 1] = (byte)((w >> 8) & 0xFF);
                bytes[o + 2] = (byte)((w >> 16) & 0xFF);
                bytes[o + 3] = (byte)((w >> 24) & 0xFF);
                o += 4;
            }

            return bytes;
        }

        /// <summary>
        /// Create a filter from a byte array previously produced by ToByteArray().
        /// </summary>
        public static SplitBlockBloomFilter FromByteArray(int numberOfBlocks, byte[] bytes) {
            if(numberOfBlocks <= 0) {
                throw new ArgumentOutOfRangeException("numberOfBlocks", "numberOfBlocks must be >= 1.");
            }
            if(bytes == null) {
                throw new ArgumentNullException("bytes");
            }
            int expected = numberOfBlocks * 32;
            if(bytes.Length != expected) {
                throw new ArgumentException("Byte array length does not match numberOfBlocks * 32.", "bytes");
            }

            SplitBlockBloomFilter f = new SplitBlockBloomFilter(numberOfBlocks);
            int len = f._words.Length;

            int o = 0;
            for(int i = 0; i < len; i++) {
                uint w =
                    (uint)bytes[o + 0] |
                    ((uint)bytes[o + 1] << 8) |
                    ((uint)bytes[o + 2] << 16) |
                    ((uint)bytes[o + 3] << 24);
                f._words[i] = w;
                o += 4;
            }

            return f;
        }

        /// <summary>
        /// Public helper for testing: returns the 8 mask words for a given low-32-bit x.
        /// Each mask has exactly one bit set.
        /// </summary>
        public static uint[] ComputeMasksFor(uint x) {
            uint[] masks = new uint[8];
            for(int i = 0; i < 8; i++) {
                uint m = unchecked(x * SplitBlockBloomFilter.Salts[i]);
                int bit = (int)(m >> 27); // top 5 bits select [0..31]
                uint mask = 1U << bit;
                masks[i] = mask;
            }
            return masks;
        }

        /// <summary>
        /// For testing/inspection: returns a copy of the 8 words for a given block index.
        /// </summary>
        public uint[] GetBlockWordsCopy(int blockIndex) {
            SplitBlockBloomFilter.ValidateBlockIndex(blockIndex, this.NumberOfBlocks);
            uint[] copy = new uint[8];
            int baseIndex = blockIndex * 8;
            for(int i = 0; i < 8; i++) {
                copy[i] = this._words[baseIndex + i];
            }
            return copy;
        }

        private static void ValidateBlockIndex(int blockIndex, int numberOfBlocks) {
            if(blockIndex < 0 || blockIndex >= numberOfBlocks) {
                throw new ArgumentOutOfRangeException("blockIndex", "blockIndex out of range.");
            }
        }

        private void BlockInsert(int blockIndex, uint x) {
            SplitBlockBloomFilter.ValidateBlockIndex(blockIndex, this.NumberOfBlocks);
            int baseIndex = blockIndex * 8;

            for(int i = 0; i < 8; i++) {
                uint m = unchecked(x * SplitBlockBloomFilter.Salts[i]);
                int bit = (int)(m >> 27);
                uint mask = 1U << bit;
                this._words[baseIndex + i] |= mask;
            }
        }

        private bool BlockCheck(int blockIndex, uint x) {
            SplitBlockBloomFilter.ValidateBlockIndex(blockIndex, this.NumberOfBlocks);
            int baseIndex = blockIndex * 8;

            for(int i = 0; i < 8; i++) {
                uint m = unchecked(x * SplitBlockBloomFilter.Salts[i]);
                int bit = (int)(m >> 27);
                uint mask = 1U << bit;
                if((this._words[baseIndex + i] & mask) == 0U) {
                    return false;
                }
            }

            return true;
        }
    }
}