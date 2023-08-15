namespace Parquet.Encodings {
    using System;
    using System.Buffers;
    using System.IO;

    //https://parquet.apache.org/docs/file-format/data-pages/encodings/#a-namedeltaencadelta-encoding-delta_binary_packed--5
    //https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingwrite.go#L287

    static partial class DeltaBinaryPackedEncoder {
        private static void Encode(ReadOnlySpan<int> data, Stream destination) {
                    
            ArrayPool<int> intPool = ArrayPool<int>.Shared;
            int blockSize = 128;
            int numMiniBlocksInBlock = 4;
            int numValuesInMiniBlock = 32;
            int totalNumValues = data.Length;

            WriteUnsignedVarInt(destination, blockSize);
            WriteUnsignedVarInt(destination, numMiniBlocksInBlock);
            WriteUnsignedVarInt(destination, totalNumValues);

            int firstValue = data[0];
            WriteZigZagVarLong(destination, firstValue);

            int i = 1;
            while(i < totalNumValues) {

                int[] rentedBlockBuf = intPool.Rent(blockSize);
                try {
                    int minDelta = int.MaxValue;
                    int blockBufCounter = 0;

                    while(i < totalNumValues && blockBufCounter < blockSize) {
                        int delta = data[i] - data[i - 1];
                        rentedBlockBuf[blockBufCounter++] = delta;
                        if(delta < minDelta) {
                            minDelta = delta;
                        }
                        i++;
                    }

                    while(blockBufCounter < blockSize) {
                        rentedBlockBuf[blockBufCounter++] = minDelta;
                    }

                    byte[] rentedBitWidths = new byte[numMiniBlocksInBlock];
                    for(int j = 0; j < numMiniBlocksInBlock; j++) {
                        int maxValue = 0;
                        for(int k = j * numValuesInMiniBlock; k < (j + 1) * numValuesInMiniBlock; k++) {
                            rentedBlockBuf[(int)k] = rentedBlockBuf[(int)k] - minDelta;
                            if(rentedBlockBuf[(int)k] > maxValue) {
                                maxValue = rentedBlockBuf[(int)k];
                            }
                        }
                        rentedBitWidths[j] = (byte)(maxValue.GetBitWidth());
                    }

                    WriteZigZagVarLong(destination, minDelta);
                    destination.Write(rentedBitWidths, 0, numMiniBlocksInBlock);

                    for(int j = 0; j < numMiniBlocksInBlock; j++) {
                        int miniBlockStart = j * numValuesInMiniBlock;
                        for(int k = miniBlockStart; k < (j + 1) * numValuesInMiniBlock; k += 8) {
                            byte[] rentedMiniBlock = new byte[rentedBitWidths[j]];
                            int end = Math.Min(8, blockSize - k);
                            BitPackedEncoder.Encode8ValuesLE(rentedBlockBuf.AsSpan(k, end), rentedMiniBlock, rentedBitWidths[j]);
                            destination.Write(rentedMiniBlock, 0, rentedBitWidths[j]);
                        }
                    }
                   
                } finally {
                    intPool.Return(rentedBlockBuf);
                }
            }
        }
        private static void Encode(ReadOnlySpan<long> data, Stream destination) {
                    
            ArrayPool<long> longPool = ArrayPool<long>.Shared;
            int blockSize = 128;
            int numMiniBlocksInBlock = 4;
            int numValuesInMiniBlock = 32;
            int totalNumValues = data.Length;

            WriteUnsignedVarInt(destination, blockSize);
            WriteUnsignedVarInt(destination, numMiniBlocksInBlock);
            WriteUnsignedVarInt(destination, totalNumValues);

            long firstValue = data[0];
            WriteZigZagVarLong(destination, firstValue);

            int i = 1;
            while(i < totalNumValues) {

                long[] rentedBlockBuf = longPool.Rent(blockSize);
                try {
                    long minDelta = long.MaxValue;
                    int blockBufCounter = 0;

                    while(i < totalNumValues && blockBufCounter < blockSize) {
                        long delta = data[i] - data[i - 1];
                        rentedBlockBuf[blockBufCounter++] = delta;
                        if(delta < minDelta) {
                            minDelta = delta;
                        }
                        i++;
                    }

                    while(blockBufCounter < blockSize) {
                        rentedBlockBuf[blockBufCounter++] = minDelta;
                    }

                    byte[] rentedBitWidths = new byte[numMiniBlocksInBlock];
                    for(int j = 0; j < numMiniBlocksInBlock; j++) {
                        long maxValue = 0;
                        for(int k = j * numValuesInMiniBlock; k < (j + 1) * numValuesInMiniBlock; k++) {
                            rentedBlockBuf[(int)k] = rentedBlockBuf[(int)k] - minDelta;
                            if(rentedBlockBuf[(int)k] > maxValue) {
                                maxValue = rentedBlockBuf[(int)k];
                            }
                        }
                        rentedBitWidths[j] = (byte)(maxValue.GetBitWidth());
                    }

                    WriteZigZagVarLong(destination, minDelta);
                    destination.Write(rentedBitWidths, 0, numMiniBlocksInBlock);

                    for(int j = 0; j < numMiniBlocksInBlock; j++) {
                        int miniBlockStart = j * numValuesInMiniBlock;
                        for(int k = miniBlockStart; k < (j + 1) * numValuesInMiniBlock; k += 8) {
                            byte[] rentedMiniBlock = new byte[rentedBitWidths[j]];
                            int end = Math.Min(8, blockSize - k);
                            BitPackedEncoder.Encode8ValuesLE(rentedBlockBuf.AsSpan(k, end), rentedMiniBlock, rentedBitWidths[j]);
                            destination.Write(rentedMiniBlock, 0, rentedBitWidths[j]);
                        }
                    }
                   
                } finally {
                    longPool.Return(rentedBlockBuf);
                }
            }
        }
    }
}