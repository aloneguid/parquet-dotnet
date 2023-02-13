

namespace Parquet.Encodings {
    using System;

    static partial class DeltaBinaryPackedEncoder {

        private static int Decode(Span<byte> s, Span<int> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = s.ReadUnsignedVarInt(ref spos);
            int miniblocksInABlock = s.ReadUnsignedVarInt(ref spos);
            int totalValueCount = s.ReadUnsignedVarInt(ref spos);       // theoretically equal to "valueCount" param
            int firstValue = (int)s.ReadZigZagVarLong(ref spos);            // the actual first value

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            int[] vbuf = new int[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            int currentValue = firstValue;
            int read = 0;
            int destOffset = 0;
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
                            dest[destOffset++] = currentValue;
                            currentValue += minDelta;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length; i++, read++) {
                            dest[destOffset++] = currentValue;
                            currentValue += minDelta + vbuf[i];
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }

        private static int Decode(Span<byte> s, Span<long> dest, out int consumedBytes) {

            int spos = 0;

            // The header is defined as follows:
            // <block size in values> <number of miniblocks in a block> <total value count> <first value>

            int blockSizeInValues = s.ReadUnsignedVarInt(ref spos);
            int miniblocksInABlock = s.ReadUnsignedVarInt(ref spos);
            int totalValueCount = s.ReadUnsignedVarInt(ref spos);       // theoretically equal to "valueCount" param
            long firstValue = (long)s.ReadZigZagVarLong(ref spos);            // the actual first value

            int valuesPerMiniblock = blockSizeInValues / miniblocksInABlock;
            long[] vbuf = new long[valuesPerMiniblock];

            // Each block contains
            // <min delta> <list of bitwidths of miniblocks> <miniblocks>

            long currentValue = firstValue;
            int read = 0;
            int destOffset = 0;
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
                            dest[destOffset++] = currentValue;
                            currentValue += minDelta;
                        }
                    } else {

                        // mini block has a size of 8*n, unpack 8 values each time
                        for(int j = 0; j < valuesPerMiniblock; j += 8) {
                            BitPackedEncoder.Unpack8ValuesLE(s.Slice(Math.Min(spos, s.Length)), vbuf.AsSpan(j), bitWidth);
                            spos += bitWidth;
                        }

                        for(int i = 0; i < vbuf.Length && destOffset < dest.Length; i++, read++) {
                            dest[destOffset++] = currentValue;
                            currentValue += minDelta + vbuf[i];
                        }

                    }
                }
            }

            consumedBytes = spos;
            return read;
        }

    }
}