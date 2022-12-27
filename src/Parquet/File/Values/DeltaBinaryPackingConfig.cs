using System;
using System.IO;

namespace Parquet.File.Values {
    /// <summary>
    /// 
    /// </summary>
    public class DeltaBinaryPackingConfig {
        internal readonly int MiniBlockNumInABlock;
        internal readonly int MiniBlockSizeInValues;

        private DeltaBinaryPackingConfig(int blockSizeInValues, int miniBlockNumInABlock) {
            MiniBlockNumInABlock = miniBlockNumInABlock;
            double miniSize = (double)blockSizeInValues / miniBlockNumInABlock;
            if(miniSize % 8 != 0) {
                throw new Exception($"miniBlockSize must be multiple of 8, but it's {miniSize}");
            }

            MiniBlockSizeInValues = (int)miniSize;
        }

        /// <summary>
        /// Read, populate and return DeltaBinaryPackingConfig
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static DeltaBinaryPackingConfig ReadConfig(BinaryReader reader) =>
            new DeltaBinaryPackingConfig(reader.ReadUnsignedVarInt(), reader.ReadUnsignedVarInt());
    }
}