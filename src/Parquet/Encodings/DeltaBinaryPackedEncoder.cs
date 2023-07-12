using System;
using System.Collections.Generic;
using System.Numerics;
using Parquet.Meta;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BINARY_PACKED (https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5)
    /// fastparquet sample: https://github.com/dask/fastparquet/blob/c59e105537a8e7673fa30676dfb16d9fa5fb1cac/fastparquet/cencoding.pyx#L232
    /// golang sample: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingread.go#L270
    /// 
    /// Supported Types: INT32, INT64
    /// </summary>
    public static partial class DeltaBinaryPackedEncoder {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <param name="dest"></param>
        /// <param name="destOffset"></param>
        /// <param name="valueCount"></param>
        /// <param name="consumedBytes"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount, out int consumedBytes) {
            System.Type? elementType = dest.GetType().GetElementType();
            if(elementType != null) {
                if(elementType == typeof(long)) {
                    Span<long> span = ((long[])dest).AsSpan(destOffset);
                    return Decode(s, span, out consumedBytes);
                } else if(elementType == typeof(int)) {
                    Span<int> span = ((int[])dest).AsSpan(destOffset);
                    return Decode(s, span, out consumedBytes);
                } else {
                    throw new NotSupportedException($"only {Parquet.Meta.Type.INT32} and {Parquet.Meta.Type.INT64} are supported in {Encoding.DELTA_BINARY_PACKED} but element type passed is {elementType}");
                }
            }

            throw new NotSupportedException($"element type {elementType} is not supported");
        }

        //https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingwrite.go#L287
        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] EncodeINT32(Array data) {
            int[] nums = (int[])data;

            List<byte> res = new List<byte>();
            int blockSize = 128;
            int numMiniBlocksInBlock = 4;
            int numValuesInMiniBlock = 32;
            int totalNumValues = nums.Length;

            int num = nums[0];
            int firstValue = ((num >> 31) ^ (num << 1));

            res.AddRange(WriteUnsignedVarInt(blockSize));
            res.AddRange(WriteUnsignedVarInt(numMiniBlocksInBlock));
            res.AddRange(WriteUnsignedVarInt(totalNumValues));
            res.AddRange(WriteUnsignedVarInt(firstValue));

            int i = 1;
            while(i < nums.Length) {
                List<int> blockBuf = new List<int>();
                int minDelta = int.MaxValue;

                while(i < nums.Length && blockBuf.Count < blockSize) {
                    int delta = nums[i] - nums[i - 1];
                    blockBuf.Add(delta);
                    if(delta < minDelta) {
                        minDelta = delta;
                    }
                    i++;
                }

                while(blockBuf.Count < blockSize) {
                    blockBuf.Add(minDelta);
                }

                byte[] bitWidths = new byte[numMiniBlocksInBlock];

                for(int j = 0; j < numMiniBlocksInBlock; j++) {
                    int maxValue = 0;
                    for(int k = j * numValuesInMiniBlock; k < (j + 1) * numValuesInMiniBlock; k++) {
                        blockBuf[(int)k] = blockBuf[(int)k] - minDelta;
                        if(blockBuf[(int)k] > maxValue) {
                            maxValue = blockBuf[(int)k];
                        }
                    }
                    bitWidths[j] = (byte)(BitOperations.Log2((uint)maxValue) + 1);
                }

                int minDeltaZigZag = ((minDelta >> 31) ^ (minDelta << 1));
                res.AddRange(WriteUnsignedVarInt(minDeltaZigZag));
                res.AddRange(bitWidths);

                for(int j = 0; j < numMiniBlocksInBlock; j++) {
                    byte[]? tempA = WriteBitPacked(blockBuf.GetRange((int)(j * numValuesInMiniBlock), (int)numValuesInMiniBlock).ToArray(), bitWidths[j], false);
                    if(tempA is not null)
                        res.AddRange(tempA);
                }
            }

            return res.ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] EncodeINT32V2(Array data) {
            int[] nums = (int[])data;

            List<byte> res = new List<byte>();
            int blockSize = 128;
            int numMiniBlocksInBlock = 4;
            int numValuesInMiniBlock = 32;
            int totalNumValues = nums.Length;

            int num = nums[0];
            int firstValue = ((num >> 31) ^ (num << 1));

            res.AddRange(WriteUnsignedVarInt(blockSize));
            res.AddRange(WriteUnsignedVarInt(numMiniBlocksInBlock));
            res.AddRange(WriteUnsignedVarInt(totalNumValues));
            res.AddRange(WriteUnsignedVarInt(firstValue));

            int i = 1;
            while(i < nums.Length) {
                List<int> blockBuf = new List<int>();
                int minDelta = int.MaxValue;

                while(i < nums.Length && blockBuf.Count < blockSize) {
                    int delta = nums[i] - nums[i - 1];
                    blockBuf.Add(delta);
                    if(delta < minDelta) {
                        minDelta = delta;
                    }
                    i++;
                }

                while(blockBuf.Count < blockSize) {
                    blockBuf.Add(minDelta);
                }


                byte[] bitWidths = new byte[numMiniBlocksInBlock];

                for(int j = 0; j < numMiniBlocksInBlock; j++) {
                    int maxValue = 0;
                    for(int k = j * numValuesInMiniBlock; k < (j + 1) * numValuesInMiniBlock; k++) {
                        blockBuf[(int)k] = blockBuf[(int)k] - minDelta;
                        if(blockBuf[(int)k] > maxValue) {
                            maxValue = blockBuf[(int)k];
                        }
                    }
                    bitWidths[j] = (byte)(BitOperations.Log2((uint)maxValue) + 1);
                }

                int minDeltaZigZag = ((minDelta >> 31) ^ (minDelta << 1));
                res.AddRange(WriteUnsignedVarInt(minDeltaZigZag));
                res.AddRange(bitWidths);

                for(int j = 0; j < numMiniBlocksInBlock; j++) {
                    int miniBlockStart = j * numValuesInMiniBlock;
                    for(int k = miniBlockStart; k < (j + 1) * numValuesInMiniBlock; k += 8) {
                        byte[] resu = new byte[bitWidths[j]];
                        int end = Math.Min(8, blockBuf.Count - k);
                        BitPackedEncoder.Encode8ValuesLE(blockBuf.GetRange(k,end).ToArray(), resu, bitWidths[j]);
                        res.AddRange(resu);
                    }
                }
            }
            return res.ToArray();
        }


        //https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingwrite.go#L216
        private static byte[]? WriteBitPacked(int[] vals, long bitWidth, bool ifHeader) {
            int ln = vals.Length;
            if(ln <= 0) {
                return null;
            }

            byte header = (byte)(((ln / 8) << 1) | 1);
            byte[] headerBuf = WriteUnsignedVarInt(header);

            List<byte> valBuf = new List<byte>();

            int i = 0;
            long resCur = 0;
            long resCurNeedBits = 8;
            long used = 0;
            long left = bitWidth - used;
            long val = vals[i];

            while(i < ln) {
                if(left >= resCurNeedBits) {
                    resCur |= ((val >> (int)used) & ((1L << (int)resCurNeedBits) - 1)) << (int)(8 - resCurNeedBits);
                    valBuf.Add((byte)resCur);
                    left -= resCurNeedBits;
                    used += resCurNeedBits;

                    resCurNeedBits = 8;
                    resCur = 0;

                    if(left <= 0 && (i + 1) < ln) {
                        i += 1;
                        val = vals[i];
                        left = bitWidth;
                        used = 0;
                    }
                } else {
                    resCur |= (val >> (int)used) << (int)(8 - resCurNeedBits);
                    i += 1;

                    if(i < ln) {
                        val = vals[i];
                    }
                    resCurNeedBits -= left;

                    left = bitWidth;
                    used = 0;
                }
            }

            List<byte> res = new List<byte>();
            if(ifHeader) {
                res.AddRange(headerBuf);
            }
            res.AddRange(valBuf.ToArray());
            return res.ToArray();
        }

        private static byte[] WriteUnsignedVarInt(int value) {
            List<byte> result = new List<byte>();
            while(value >= 0x80) {
                result.Add((byte)(value | 0x80));
                value >>= 7;
            }
            result.Add((byte)value);
            return result.ToArray();
        }
    }
}
