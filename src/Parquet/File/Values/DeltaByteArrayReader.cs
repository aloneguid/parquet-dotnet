using System;
using System.IO;

namespace Parquet.File.Values {
    /// <summary>
    /// 
    /// </summary>
    public static class DeltaByteArrayReader {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="dest"></param>
        /// <param name="valueCount"></param>
        public static void Read(BinaryReader reader, Array dest, int valueCount) {
            string[] result = (string[])dest;
            byte[] previous = Array.Empty<byte>();
            for(int i = 0; i < valueCount; i++) {
                var prefixLengthReader = DeltaBinaryPackingValuesReader.GetDeltaBinaryPackingValuesReader(reader); 
                int prefixLength = prefixLengthReader.ReadInteger();

                var suffixReader = DeltaLengthByteArrayValuesReader.GetDeltaLengthByteArrayValuesReader(reader);
                byte[] suffix = suffixReader.readBytes();

                int length = prefixLength + suffix.Length;

                if(prefixLength != 0) {
                    byte[] value = new byte[length];
                    Array.Copy(previous, 0, value, 0, prefixLength);
                    Array.Copy(suffix, 0, value, prefixLength, suffix.Length);

                    result[i] = System.Text.Encoding.UTF8.GetString(value);

                    previous = value;
                }
                else {
                    previous = suffix;
                }
            }
        }
    }
    
}