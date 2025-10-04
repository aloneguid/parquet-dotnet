namespace Parquet.Encodings {
    using System;
    using System.IO;
    using Parquet.Extensions;

    static partial class DeltaBinaryPackedEncoder {

        private static void FlushShortBlock(Span<short> block, short minDelta,
            Stream destination,
            int miniblockCount, int miniblockSize) {

            // min delta can be flushed immediately
            destination.WriteULEB128((ulong)((long)minDelta).ZigZagEncode());

            // subtract minDelta from all values
            for(int i = 0; i < block.Length; i++) {
                block[i] = (short)(block[i] - minDelta);
            }

            // we need bit widths for each miniblock (after minDelta is applied)
            Span<byte> bitWidths = stackalloc byte[miniblockCount];

            for(int offset = 0, bwi = 0; offset < block.Length; offset += miniblockSize, bwi++) {
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 0)
                    break;

                int bitwidth = block.Slice(offset, count).CalculateBitWidth();
                bitWidths[bwi] = (byte)bitwidth;
            }

            // write bit widths
            destination.WriteSpan(bitWidths);

            // each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
            Span<int> raw8 = stackalloc int[8];
            for(int i = 0; i < miniblockCount; i++) {
                int offset = i * miniblockSize;
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 1)
                    break;
                Span<short> miniblockData = block.Slice(offset, count);
                // write values in 8
                int bitWidth = bitWidths[i];
                byte[] encoded8 = new byte[bitWidth];
                for(int iv = 0; iv < miniblockData.Length; iv += 8) {
                    int count8 = Math.Min(8, miniblockData.Length - iv);
                    // Convert to bit packing type for bit packing
                    for(int j = 0; j < count8; j++) {
                        raw8[j] = (int)miniblockData[iv + j];
                    }
                    for(int j = count8; j < 8; j++) {
                        raw8[j] = 0;
                    }
                    BitPackedEncoder.Pack8ValuesLE(raw8, encoded8, bitWidth);
                    destination.Write(encoded8, 0, bitWidth);
                }
            }
        }

        private static void EncodeShort(ReadOnlySpan<short> data, Stream destination,
            int blockSize, int miniblockSize) {

            if(data.Length == 0)
                return;

            // header: <block size in values> <number of miniblocks in a block> <total value count> <first value>
            int miniblockCount = blockSize / miniblockSize;
            destination.WriteULEB128((ulong)blockSize);
            destination.WriteULEB128((ulong)miniblockCount);
            destination.WriteULEB128((ulong)data.Length);
            destination.WriteULEB128((ulong)((long)data[0]).ZigZagEncode());

            // each block: <min delta> <list of bitwidths of miniblocks> <miniblocks>
            Span<short> block = stackalloc short[blockSize];
            int blockCount = 0;
            short minDelta = 0;
            for(int i = 1; i < data.Length; i++) {

                // calculate delta element and minDelta
                short delta = (short)(data[i] - data[i - 1]);
                if(blockCount == 0 || delta < minDelta) {
                    minDelta = delta;
                }
                block[blockCount++] = delta;

                // write block
                if(blockCount == blockSize) {
                    FlushShortBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
                    blockCount = 0;
                }
            }

            if(blockCount > 0) {
                while(blockCount < blockSize) {
                    block[blockCount++] = minDelta;
                }
                FlushShortBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
            }
        }

        private static int DecodeShort(Span<byte> s, Span<short> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = (int)s.ULEB128Decode(ref spos);
            int miniblocksInABlock = (int)s.ULEB128Decode(ref spos);
            int totalValueCount = (int)s.ULEB128Decode(ref spos);           // theoretically equal to "valueCount" param
            short firstValue = (short)s.ReadZigZagVarLong(ref spos);            // the actual first value

            if(totalValueCount == 0) {
                consumedBytes = spos;
                return 0;
            } else if(totalValueCount == 1) {
                dest[0] = firstValue;
                consumedBytes = spos;
                return 1;
            }

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            int[] vbuf = new int[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            short currentValue = firstValue;
            int read = 1;
            int destOffset = 0;
            dest[destOffset++] = firstValue;
            while(read < totalValueCount && spos < s.Length) {
                short minDelta = (short)s.ReadZigZagVarLong(ref spos);

                Span<byte> bitWidths = s.Slice(spos, Math.Min(miniblocksInABlock, s.Length - spos));
                spos += miniblocksInABlock;
                foreach(byte bitWidth in bitWidths) {

                    // unpack miniblock

                    if(read >= totalValueCount)
                        break;

                    if(bitWidth == 0) {
                        // there's not data for bitwidth 0
                        for(int i = 0; i < valuesPerMiniblock && destOffset < dest.Length; i++, read++) {
                            if(read >= totalValueCount)
                                break;
                            currentValue = (short)(currentValue + minDelta);
                            dest[destOffset++] = currentValue;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock && spos < s.Length; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length && read < totalValueCount; i++, read++) {
                            currentValue = (short)(currentValue + minDelta + (short)vbuf[i]);
                            dest[destOffset++] = currentValue;
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }

        private static void FlushUshortBlock(Span<ushort> block, ushort minDelta,
            Stream destination,
            int miniblockCount, int miniblockSize) {

            // min delta can be flushed immediately
            destination.WriteULEB128((ulong)((long)minDelta).ZigZagEncode());

            // subtract minDelta from all values
            for(int i = 0; i < block.Length; i++) {
                block[i] = (ushort)(block[i] - minDelta);
            }

            // we need bit widths for each miniblock (after minDelta is applied)
            Span<byte> bitWidths = stackalloc byte[miniblockCount];

            for(int offset = 0, bwi = 0; offset < block.Length; offset += miniblockSize, bwi++) {
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 0)
                    break;

                int bitwidth = block.Slice(offset, count).CalculateBitWidth();
                bitWidths[bwi] = (byte)bitwidth;
            }

            // write bit widths
            destination.WriteSpan(bitWidths);

            // each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
            Span<int> raw8 = stackalloc int[8];
            for(int i = 0; i < miniblockCount; i++) {
                int offset = i * miniblockSize;
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 1)
                    break;
                Span<ushort> miniblockData = block.Slice(offset, count);
                // write values in 8
                int bitWidth = bitWidths[i];
                byte[] encoded8 = new byte[bitWidth];
                for(int iv = 0; iv < miniblockData.Length; iv += 8) {
                    int count8 = Math.Min(8, miniblockData.Length - iv);
                    // Convert to bit packing type for bit packing
                    for(int j = 0; j < count8; j++) {
                        raw8[j] = (int)miniblockData[iv + j];
                    }
                    for(int j = count8; j < 8; j++) {
                        raw8[j] = 0;
                    }
                    BitPackedEncoder.Pack8ValuesLE(raw8, encoded8, bitWidth);
                    destination.Write(encoded8, 0, bitWidth);
                }
            }
        }

        private static void EncodeUshort(ReadOnlySpan<ushort> data, Stream destination,
            int blockSize, int miniblockSize) {

            if(data.Length == 0)
                return;

            // header: <block size in values> <number of miniblocks in a block> <total value count> <first value>
            int miniblockCount = blockSize / miniblockSize;
            destination.WriteULEB128((ulong)blockSize);
            destination.WriteULEB128((ulong)miniblockCount);
            destination.WriteULEB128((ulong)data.Length);
            destination.WriteULEB128((ulong)((long)data[0]).ZigZagEncode());

            // each block: <min delta> <list of bitwidths of miniblocks> <miniblocks>
            Span<ushort> block = stackalloc ushort[blockSize];
            int blockCount = 0;
            ushort minDelta = 0;
            for(int i = 1; i < data.Length; i++) {

                // calculate delta element and minDelta
                ushort delta = (ushort)(data[i] - data[i - 1]);
                if(blockCount == 0 || delta < minDelta) {
                    minDelta = delta;
                }
                block[blockCount++] = delta;

                // write block
                if(blockCount == blockSize) {
                    FlushUshortBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
                    blockCount = 0;
                }
            }

            if(blockCount > 0) {
                while(blockCount < blockSize) {
                    block[blockCount++] = minDelta;
                }
                FlushUshortBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
            }
        }

        private static int DecodeUshort(Span<byte> s, Span<ushort> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = (int)s.ULEB128Decode(ref spos);
            int miniblocksInABlock = (int)s.ULEB128Decode(ref spos);
            int totalValueCount = (int)s.ULEB128Decode(ref spos);           // theoretically equal to "valueCount" param
            ushort firstValue = (ushort)s.ReadZigZagVarLong(ref spos);            // the actual first value

            if(totalValueCount == 0) {
                consumedBytes = spos;
                return 0;
            } else if(totalValueCount == 1) {
                dest[0] = firstValue;
                consumedBytes = spos;
                return 1;
            }

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            int[] vbuf = new int[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            ushort currentValue = firstValue;
            int read = 1;
            int destOffset = 0;
            dest[destOffset++] = firstValue;
            while(read < totalValueCount && spos < s.Length) {
                ushort minDelta = (ushort)s.ReadZigZagVarLong(ref spos);

                Span<byte> bitWidths = s.Slice(spos, Math.Min(miniblocksInABlock, s.Length - spos));
                spos += miniblocksInABlock;
                foreach(byte bitWidth in bitWidths) {

                    // unpack miniblock

                    if(read >= totalValueCount)
                        break;

                    if(bitWidth == 0) {
                        // there's not data for bitwidth 0
                        for(int i = 0; i < valuesPerMiniblock && destOffset < dest.Length; i++, read++) {
                            if(read >= totalValueCount)
                                break;
                            currentValue = (ushort)(currentValue + minDelta);
                            dest[destOffset++] = currentValue;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock && spos < s.Length; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length && read < totalValueCount; i++, read++) {
                            currentValue = (ushort)(currentValue + minDelta + (ushort)vbuf[i]);
                            dest[destOffset++] = currentValue;
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }

        private static void FlushIntBlock(Span<int> block, int minDelta,
            Stream destination,
            int miniblockCount, int miniblockSize) {

            // min delta can be flushed immediately
            destination.WriteULEB128((ulong)((long)minDelta).ZigZagEncode());

            // subtract minDelta from all values
            for(int i = 0; i < block.Length; i++) {
                block[i] = block[i] - minDelta;
            }

            // we need bit widths for each miniblock (after minDelta is applied)
            Span<byte> bitWidths = stackalloc byte[miniblockCount];

            for(int offset = 0, bwi = 0; offset < block.Length; offset += miniblockSize, bwi++) {
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 0)
                    break;

                int bitwidth = block.Slice(offset, count).CalculateBitWidth();
                bitWidths[bwi] = (byte)bitwidth;
            }

            // write bit widths
            destination.WriteSpan(bitWidths);

            // each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
            Span<int> raw8 = stackalloc int[8];
            for(int i = 0; i < miniblockCount; i++) {
                int offset = i * miniblockSize;
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 1)
                    break;
                Span<int> miniblockData = block.Slice(offset, count);
                // write values in 8
                int bitWidth = bitWidths[i];
                byte[] encoded8 = new byte[bitWidth];
                for(int iv = 0; iv < miniblockData.Length; iv += 8) {
                    int count8 = Math.Min(8, miniblockData.Length - iv);
                    miniblockData.Slice(iv, count8).CopyTo(raw8);
                    BitPackedEncoder.Pack8ValuesLE(raw8, encoded8, bitWidth);
                    destination.Write(encoded8, 0, bitWidth);
                }
            }
        }

        private static void EncodeInt(ReadOnlySpan<int> data, Stream destination,
            int blockSize, int miniblockSize) {

            if(data.Length == 0)
                return;

            // header: <block size in values> <number of miniblocks in a block> <total value count> <first value>
            int miniblockCount = blockSize / miniblockSize;
            destination.WriteULEB128((ulong)blockSize);
            destination.WriteULEB128((ulong)miniblockCount);
            destination.WriteULEB128((ulong)data.Length);
            destination.WriteULEB128((ulong)((long)data[0]).ZigZagEncode());

            // each block: <min delta> <list of bitwidths of miniblocks> <miniblocks>
            Span<int> block = stackalloc int[blockSize];
            int blockCount = 0;
            int minDelta = 0;
            for(int i = 1; i < data.Length; i++) {

                // calculate delta element and minDelta
                int delta = data[i] - data[i - 1];
                if(blockCount == 0 || delta < minDelta) {
                    minDelta = delta;
                }
                block[blockCount++] = delta;

                // write block
                if(blockCount == blockSize) {
                    FlushIntBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
                    blockCount = 0;
                }
            }

            if(blockCount > 0) {
                while(blockCount < blockSize) {
                    block[blockCount++] = minDelta;
                }
                FlushIntBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
            }
        }

        private static int DecodeInt(Span<byte> s, Span<int> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = (int)s.ULEB128Decode(ref spos);
            int miniblocksInABlock = (int)s.ULEB128Decode(ref spos);
            int totalValueCount = (int)s.ULEB128Decode(ref spos);           // theoretically equal to "valueCount" param
            int firstValue = (int)s.ReadZigZagVarLong(ref spos);            // the actual first value

            if(totalValueCount == 0) {
                consumedBytes = spos;
                return 0;
            } else if(totalValueCount == 1) {
                dest[0] = firstValue;
                consumedBytes = spos;
                return 1;
            }

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            int[] vbuf = new int[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            int currentValue = firstValue;
            int read = 1;
            int destOffset = 0;
            dest[destOffset++] = firstValue;
            while(read < totalValueCount && spos < s.Length) {
                int minDelta = (int)s.ReadZigZagVarLong(ref spos);

                Span<byte> bitWidths = s.Slice(spos, Math.Min(miniblocksInABlock, s.Length - spos));
                spos += miniblocksInABlock;
                foreach(byte bitWidth in bitWidths) {

                    // unpack miniblock

                    if(read >= totalValueCount)
                        break;

                    if(bitWidth == 0) {
                        // there's not data for bitwidth 0
                        for(int i = 0; i < valuesPerMiniblock && destOffset < dest.Length; i++, read++) {
                            if(read >= totalValueCount)
                                break;
                            currentValue += minDelta;
                            dest[destOffset++] = currentValue;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock && spos < s.Length; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length && read < totalValueCount; i++, read++) {
                            currentValue += minDelta + vbuf[i];
                            dest[destOffset++] = currentValue;
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }

        private static void FlushUintBlock(Span<uint> block, uint minDelta,
            Stream destination,
            int miniblockCount, int miniblockSize) {

            // min delta can be flushed immediately
            destination.WriteULEB128((ulong)((long)minDelta).ZigZagEncode());

            // subtract minDelta from all values
            for(int i = 0; i < block.Length; i++) {
                block[i] = block[i] - minDelta;
            }

            // we need bit widths for each miniblock (after minDelta is applied)
            Span<byte> bitWidths = stackalloc byte[miniblockCount];

            for(int offset = 0, bwi = 0; offset < block.Length; offset += miniblockSize, bwi++) {
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 0)
                    break;

                int bitwidth = block.Slice(offset, count).CalculateBitWidth();
                bitWidths[bwi] = (byte)bitwidth;
            }

            // write bit widths
            destination.WriteSpan(bitWidths);

            // each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
            Span<long> raw8 = stackalloc long[8];
            for(int i = 0; i < miniblockCount; i++) {
                int offset = i * miniblockSize;
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 1)
                    break;
                Span<uint> miniblockData = block.Slice(offset, count);
                // write values in 8
                int bitWidth = bitWidths[i];
                byte[] encoded8 = new byte[bitWidth];
                for(int iv = 0; iv < miniblockData.Length; iv += 8) {
                    int count8 = Math.Min(8, miniblockData.Length - iv);
                    // Convert to bit packing type for bit packing
                    for(int j = 0; j < count8; j++) {
                        raw8[j] = (long)miniblockData[iv + j];
                    }
                    for(int j = count8; j < 8; j++) {
                        raw8[j] = 0;
                    }
                    BitPackedEncoder.Pack8ValuesLE(raw8, encoded8, bitWidth);
                    destination.Write(encoded8, 0, bitWidth);
                }
            }
        }

        private static void EncodeUint(ReadOnlySpan<uint> data, Stream destination,
            int blockSize, int miniblockSize) {

            if(data.Length == 0)
                return;

            // header: <block size in values> <number of miniblocks in a block> <total value count> <first value>
            int miniblockCount = blockSize / miniblockSize;
            destination.WriteULEB128((ulong)blockSize);
            destination.WriteULEB128((ulong)miniblockCount);
            destination.WriteULEB128((ulong)data.Length);
            destination.WriteULEB128((ulong)((long)data[0]).ZigZagEncode());

            // each block: <min delta> <list of bitwidths of miniblocks> <miniblocks>
            Span<uint> block = stackalloc uint[blockSize];
            int blockCount = 0;
            uint minDelta = 0;
            for(int i = 1; i < data.Length; i++) {

                // calculate delta element and minDelta
                uint delta = data[i] - data[i - 1];
                if(blockCount == 0 || delta < minDelta) {
                    minDelta = delta;
                }
                block[blockCount++] = delta;

                // write block
                if(blockCount == blockSize) {
                    FlushUintBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
                    blockCount = 0;
                }
            }

            if(blockCount > 0) {
                while(blockCount < blockSize) {
                    block[blockCount++] = minDelta;
                }
                FlushUintBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
            }
        }

        private static int DecodeUint(Span<byte> s, Span<uint> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = (int)s.ULEB128Decode(ref spos);
            int miniblocksInABlock = (int)s.ULEB128Decode(ref spos);
            int totalValueCount = (int)s.ULEB128Decode(ref spos);           // theoretically equal to "valueCount" param
            uint firstValue = (uint)s.ReadZigZagVarLong(ref spos);            // the actual first value

            if(totalValueCount == 0) {
                consumedBytes = spos;
                return 0;
            } else if(totalValueCount == 1) {
                dest[0] = firstValue;
                consumedBytes = spos;
                return 1;
            }

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            long[] vbuf = new long[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            uint currentValue = firstValue;
            int read = 1;
            int destOffset = 0;
            dest[destOffset++] = firstValue;
            while(read < totalValueCount && spos < s.Length) {
                uint minDelta = (uint)s.ReadZigZagVarLong(ref spos);

                Span<byte> bitWidths = s.Slice(spos, Math.Min(miniblocksInABlock, s.Length - spos));
                spos += miniblocksInABlock;
                foreach(byte bitWidth in bitWidths) {

                    // unpack miniblock

                    if(read >= totalValueCount)
                        break;

                    if(bitWidth == 0) {
                        // there's not data for bitwidth 0
                        for(int i = 0; i < valuesPerMiniblock && destOffset < dest.Length; i++, read++) {
                            if(read >= totalValueCount)
                                break;
                            currentValue += minDelta;
                            dest[destOffset++] = currentValue;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock && spos < s.Length; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length && read < totalValueCount; i++, read++) {
                            currentValue += (uint)(minDelta + (uint)vbuf[i]);
                            dest[destOffset++] = currentValue;
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }

        private static void FlushLongBlock(Span<long> block, long minDelta,
            Stream destination,
            int miniblockCount, int miniblockSize) {

            // min delta can be flushed immediately
            destination.WriteULEB128((ulong)((long)minDelta).ZigZagEncode());

            // subtract minDelta from all values
            for(int i = 0; i < block.Length; i++) {
                block[i] = block[i] - minDelta;
            }

            // we need bit widths for each miniblock (after minDelta is applied)
            Span<byte> bitWidths = stackalloc byte[miniblockCount];

            for(int offset = 0, bwi = 0; offset < block.Length; offset += miniblockSize, bwi++) {
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 0)
                    break;

                int bitwidth = block.Slice(offset, count).CalculateBitWidth();
                bitWidths[bwi] = (byte)bitwidth;
            }

            // write bit widths
            destination.WriteSpan(bitWidths);

            // each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
            Span<long> raw8 = stackalloc long[8];
            for(int i = 0; i < miniblockCount; i++) {
                int offset = i * miniblockSize;
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 1)
                    break;
                Span<long> miniblockData = block.Slice(offset, count);
                // write values in 8
                int bitWidth = bitWidths[i];
                byte[] encoded8 = new byte[bitWidth];
                for(int iv = 0; iv < miniblockData.Length; iv += 8) {
                    int count8 = Math.Min(8, miniblockData.Length - iv);
                    miniblockData.Slice(iv, count8).CopyTo(raw8);
                    BitPackedEncoder.Pack8ValuesLE(raw8, encoded8, bitWidth);
                    destination.Write(encoded8, 0, bitWidth);
                }
            }
        }

        private static void EncodeLong(ReadOnlySpan<long> data, Stream destination,
            int blockSize, int miniblockSize) {

            if(data.Length == 0)
                return;

            // header: <block size in values> <number of miniblocks in a block> <total value count> <first value>
            int miniblockCount = blockSize / miniblockSize;
            destination.WriteULEB128((ulong)blockSize);
            destination.WriteULEB128((ulong)miniblockCount);
            destination.WriteULEB128((ulong)data.Length);
            destination.WriteULEB128((ulong)((long)data[0]).ZigZagEncode());

            // each block: <min delta> <list of bitwidths of miniblocks> <miniblocks>
            Span<long> block = stackalloc long[blockSize];
            int blockCount = 0;
            long minDelta = 0;
            for(int i = 1; i < data.Length; i++) {

                // calculate delta element and minDelta
                long delta = data[i] - data[i - 1];
                if(blockCount == 0 || delta < minDelta) {
                    minDelta = delta;
                }
                block[blockCount++] = delta;

                // write block
                if(blockCount == blockSize) {
                    FlushLongBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
                    blockCount = 0;
                }
            }

            if(blockCount > 0) {
                while(blockCount < blockSize) {
                    block[blockCount++] = minDelta;
                }
                FlushLongBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
            }
        }

        private static int DecodeLong(Span<byte> s, Span<long> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = (int)s.ULEB128Decode(ref spos);
            int miniblocksInABlock = (int)s.ULEB128Decode(ref spos);
            int totalValueCount = (int)s.ULEB128Decode(ref spos);           // theoretically equal to "valueCount" param
            long firstValue = (long)s.ReadZigZagVarLong(ref spos);            // the actual first value

            if(totalValueCount == 0) {
                consumedBytes = spos;
                return 0;
            } else if(totalValueCount == 1) {
                dest[0] = firstValue;
                consumedBytes = spos;
                return 1;
            }

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            long[] vbuf = new long[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            long currentValue = firstValue;
            int read = 1;
            int destOffset = 0;
            dest[destOffset++] = firstValue;
            while(read < totalValueCount && spos < s.Length) {
                long minDelta = (long)s.ReadZigZagVarLong(ref spos);

                Span<byte> bitWidths = s.Slice(spos, Math.Min(miniblocksInABlock, s.Length - spos));
                spos += miniblocksInABlock;
                foreach(byte bitWidth in bitWidths) {

                    // unpack miniblock

                    if(read >= totalValueCount)
                        break;

                    if(bitWidth == 0) {
                        // there's not data for bitwidth 0
                        for(int i = 0; i < valuesPerMiniblock && destOffset < dest.Length; i++, read++) {
                            if(read >= totalValueCount)
                                break;
                            currentValue += minDelta;
                            dest[destOffset++] = currentValue;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock && spos < s.Length; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length && read < totalValueCount; i++, read++) {
                            currentValue += minDelta + vbuf[i];
                            dest[destOffset++] = currentValue;
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }

        private static void FlushUlongBlock(Span<ulong> block, ulong minDelta,
            Stream destination,
            int miniblockCount, int miniblockSize) {

            // min delta can be flushed immediately
            destination.WriteULEB128((ulong)((long)minDelta).ZigZagEncode());

            // subtract minDelta from all values
            for(int i = 0; i < block.Length; i++) {
                block[i] = block[i] - minDelta;
            }

            // we need bit widths for each miniblock (after minDelta is applied)
            Span<byte> bitWidths = stackalloc byte[miniblockCount];

            for(int offset = 0, bwi = 0; offset < block.Length; offset += miniblockSize, bwi++) {
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 0)
                    break;

                int bitwidth = block.Slice(offset, count).CalculateBitWidth();
                bitWidths[bwi] = (byte)bitwidth;
            }

            // write bit widths
            destination.WriteSpan(bitWidths);

            // each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
            Span<long> raw8 = stackalloc long[8];
            for(int i = 0; i < miniblockCount; i++) {
                int offset = i * miniblockSize;
                int count = Math.Min(miniblockSize, block.Length - offset);
                if(count < 1)
                    break;
                Span<ulong> miniblockData = block.Slice(offset, count);
                // write values in 8
                int bitWidth = bitWidths[i];
                byte[] encoded8 = new byte[bitWidth];
                for(int iv = 0; iv < miniblockData.Length; iv += 8) {
                    int count8 = Math.Min(8, miniblockData.Length - iv);
                    // Convert to bit packing type for bit packing
                    for(int j = 0; j < count8; j++) {
                        raw8[j] = (long)miniblockData[iv + j];
                    }
                    for(int j = count8; j < 8; j++) {
                        raw8[j] = 0;
                    }
                    BitPackedEncoder.Pack8ValuesLE(raw8, encoded8, bitWidth);
                    destination.Write(encoded8, 0, bitWidth);
                }
            }
        }

        private static void EncodeUlong(ReadOnlySpan<ulong> data, Stream destination,
            int blockSize, int miniblockSize) {

            if(data.Length == 0)
                return;

            // header: <block size in values> <number of miniblocks in a block> <total value count> <first value>
            int miniblockCount = blockSize / miniblockSize;
            destination.WriteULEB128((ulong)blockSize);
            destination.WriteULEB128((ulong)miniblockCount);
            destination.WriteULEB128((ulong)data.Length);
            destination.WriteULEB128((ulong)((long)data[0]).ZigZagEncode());

            // each block: <min delta> <list of bitwidths of miniblocks> <miniblocks>
            Span<ulong> block = stackalloc ulong[blockSize];
            int blockCount = 0;
            ulong minDelta = 0;
            for(int i = 1; i < data.Length; i++) {

                // calculate delta element and minDelta
                ulong delta = data[i] - data[i - 1];
                if(blockCount == 0 || delta < minDelta) {
                    minDelta = delta;
                }
                block[blockCount++] = delta;

                // write block
                if(blockCount == blockSize) {
                    FlushUlongBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
                    blockCount = 0;
                }
            }

            if(blockCount > 0) {
                while(blockCount < blockSize) {
                    block[blockCount++] = minDelta;
                }
                FlushUlongBlock(block.Slice(0, blockCount), minDelta, destination, miniblockCount, miniblockSize);
            }
        }

        private static int DecodeUlong(Span<byte> s, Span<ulong> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = (int)s.ULEB128Decode(ref spos);
            int miniblocksInABlock = (int)s.ULEB128Decode(ref spos);
            int totalValueCount = (int)s.ULEB128Decode(ref spos);           // theoretically equal to "valueCount" param
            ulong firstValue = (ulong)s.ReadZigZagVarLong(ref spos);            // the actual first value

            if(totalValueCount == 0) {
                consumedBytes = spos;
                return 0;
            } else if(totalValueCount == 1) {
                dest[0] = firstValue;
                consumedBytes = spos;
                return 1;
            }

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            long[] vbuf = new long[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            ulong currentValue = firstValue;
            int read = 1;
            int destOffset = 0;
            dest[destOffset++] = firstValue;
            while(read < totalValueCount && spos < s.Length) {
                ulong minDelta = (ulong)s.ReadZigZagVarLong(ref spos);

                Span<byte> bitWidths = s.Slice(spos, Math.Min(miniblocksInABlock, s.Length - spos));
                spos += miniblocksInABlock;
                foreach(byte bitWidth in bitWidths) {

                    // unpack miniblock

                    if(read >= totalValueCount)
                        break;

                    if(bitWidth == 0) {
                        // there's not data for bitwidth 0
                        for(int i = 0; i < valuesPerMiniblock && destOffset < dest.Length; i++, read++) {
                            if(read >= totalValueCount)
                                break;
                            currentValue += minDelta;
                            dest[destOffset++] = currentValue;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock && spos < s.Length; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length && read < totalValueCount; i++, read++) {
                            currentValue += (ulong)(minDelta + (ulong)vbuf[i]);
                            dest[destOffset++] = currentValue;
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }
    }
}