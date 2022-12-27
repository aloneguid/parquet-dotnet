using System.IO;

namespace Parquet.File.Values
{
    /// <summary>
    /// 
    /// </summary>
    public class DeltaLengthByteArrayValuesReader {
        private readonly BinaryReader _reader;
        private readonly DeltaBinaryPackingValuesReader _lengthReader;

        private DeltaLengthByteArrayValuesReader(BinaryReader reader, DeltaBinaryPackingValuesReader lengthReader) {
            _reader = reader;
            _lengthReader = lengthReader;
        }

        /// <summary>
        /// Reads a sequence of <see cref="byte"/> values.
        /// </summary>
        /// <returns></returns>
        public byte[] ReadBytes() {
            int length = _lengthReader.ReadInteger();

            return _reader.ReadBytes(length);
        }
        
        /// <summary>
        /// Gets and Initializes the configuration for the reader
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static DeltaLengthByteArrayValuesReader GetDeltaLengthByteArrayValuesReader(BinaryReader reader) {
            var deltaBinaryPackingValuesReader = DeltaBinaryPackingValuesReader.GetDeltaBinaryPackingValuesReader(reader);
            return new DeltaLengthByteArrayValuesReader(reader, deltaBinaryPackingValuesReader);
        }
        
    }
}