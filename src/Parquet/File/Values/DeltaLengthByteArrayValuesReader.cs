using System.IO;

namespace Parquet.File.Values
{
    /// <summary>
    /// 
    /// </summary>
    public class DeltaLengthByteArrayValuesReader {
        private readonly BinaryReader _reader;
        private readonly DeltaBinaryPackingValuesReader _lengthReader;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="lengthReader"></param>
        public DeltaLengthByteArrayValuesReader(BinaryReader reader, DeltaBinaryPackingValuesReader lengthReader) {
            _reader = reader;
            _lengthReader = lengthReader;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public byte[] readBytes() {
            int length = _lengthReader.ReadInteger();

            return _reader.ReadBytes(length);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static DeltaLengthByteArrayValuesReader GetDeltaLengthByteArrayValuesReader(BinaryReader reader) {
            var deltaBinaryPackingValuesReader = DeltaBinaryPackingValuesReader.GetDeltaBinaryPackingValuesReader(reader);
            return new DeltaLengthByteArrayValuesReader(reader, deltaBinaryPackingValuesReader);
        }
        
    }
}