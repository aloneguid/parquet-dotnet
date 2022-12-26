using System.IO;

namespace Parquet {
    /// <summary>
    /// 
    /// </summary>
    public static class BinaryReaderExtensions {
        /// <summary>
        /// Read a value using the unsigned, variable int encoding.
        /// </summary>
        public static int ReadUnsignedVarInt(this BinaryReader reader) {
            int result = 0;
            int shift = 0;

            while(true) {
                byte b = reader.ReadByte();
                result |= ((b & 0x7F) << shift);
                if((b & 0x80) == 0) break;
                shift += 7;
            }

            return result;
        }

        /// <summary>
        /// Read a value using the unsigned, variable long encoding. 
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static long ReadUnsignedVarLong(this BinaryReader reader) {
            long value = 0;
            int i = 0;
            long b;
            while(((b = reader.ReadByte()) &0x80) != 0) {
                value |= (b & 0x7F) << i;
                i += 7;
            }
            return value | (b << i);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static long ReadZigZagVarLong(this BinaryReader reader) {
            long raw = reader.ReadUnsignedVarLong();
            long temp = (((raw << 63) >> 63) ^ raw) >> 1;
            return temp ^ (raw & (1L << 63));
        }
    }
}