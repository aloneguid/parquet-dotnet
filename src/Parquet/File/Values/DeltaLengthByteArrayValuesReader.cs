using System.IO;
using Parquet.Extensions;

namespace Parquet.File.Values
{
    /// <summary>
    /// 
    /// </summary>
    public class DeltaLengthByteArrayValuesReader {
        private readonly Stream _stream;
        private readonly DeltaBinaryPackingValuesReader _lengthReader;

        private DeltaLengthByteArrayValuesReader(Stream s, DeltaBinaryPackingValuesReader lengthReader) {
            _stream = s;
            _lengthReader = lengthReader;
        }

        /// <summary>
        /// Reads a sequence of <see cref="byte"/> values.
        /// </summary>
        /// <returns></returns>
        public byte[] ReadBytes() {
            int length = _lengthReader.ReadInteger();

            return _stream.ReadBytesExactly(length);
        }
        
        /// <summary>
        /// Gets and Initializes the configuration for the reader
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DeltaLengthByteArrayValuesReader GetDeltaLengthByteArrayValuesReader(Stream s) {
            var deltaBinaryPackingValuesReader = DeltaBinaryPackingValuesReader.GetDeltaBinaryPackingValuesReader(s);
            return new DeltaLengthByteArrayValuesReader(s, deltaBinaryPackingValuesReader);
        }
        
    }
}