using System;
using System.IO;

namespace Parquet.File.Values {
    /// <summary>
    /// 
    /// </summary>
    public static class DeltaByteArrayReader {
        /// <summary>
        ///      From documentation:
        ///      This is also known as incremental encoding or front compression: for each element in a sequence of strings, store the prefix length of the previous entry plus the suffix.
        ///      For a longer description, see https://en.wikipedia.org/wiki/Incremental_encoding.
        ///      This is stored as a sequence of delta-encoded prefix lengths (DELTA_BINARY_PACKED), followed by the suffixes encoded as delta length byte arrays (DELTA_LENGTH_BYTE_ARRAY).
        /// </summary>
        /// <param name="s"></param>
        /// <param name="dest"></param>
        /// <param name="offset"></param>
        /// <param name="valueCount"></param>
        public static int Read(Stream s, Array dest, int offset, int valueCount) {
            var prefixLengthReader = DeltaBinaryPackingValuesReader.GetDeltaBinaryPackingValuesReader(s);
            var suffixReader = DeltaLengthByteArrayValuesReader.GetDeltaLengthByteArrayValuesReader(s);
            
            string[] result = (string[])dest;
            byte[] previous = Array.Empty<byte>();
            
            for(int i = 0; i < valueCount; i++) {
                int prefixLength = prefixLengthReader.ReadInteger();
                byte[] suffix = suffixReader.ReadBytes();

                int length = prefixLength + suffix.Length;

                if(prefixLength != 0) {
                    byte[] value = new byte[length];
                    Array.Copy(previous, 0, value, 0, prefixLength);
                    Array.Copy(suffix, 0, value, prefixLength, suffix.Length);

                    result[offset + i] = System.Text.Encoding.UTF8.GetString(value);
                    previous = value;
                }
                else {
                    result[offset + i] = System.Text.Encoding.UTF8.GetString(suffix);
                    previous = suffix;
                }
            }

            return valueCount;
        }
    }
    
}