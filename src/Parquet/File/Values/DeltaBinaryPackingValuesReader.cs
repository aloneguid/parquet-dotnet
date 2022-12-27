using System;
using System.IO;

namespace Parquet.File.Values {
    /// <summary>
    /// 
    /// </summary>
    public class DeltaBinaryPackingValuesReader {
        private readonly BinaryReader _reader;
        private DeltaBinaryPackingConfig _config;
        private int _totalValueCount;
        private long[] _valuesBuffer;
        private int _valuesBuffered = 0;
        private int[] _bitWidths;
        private int _valuesRead = 0;

        private DeltaBinaryPackingValuesReader(BinaryReader reader) => _reader = reader;

        /// <summary>
        /// Gets and Initializes the configuration for the reader
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static DeltaBinaryPackingValuesReader GetDeltaBinaryPackingValuesReader(BinaryReader reader) {
            var deltaBinaryPackingValuesReader = new DeltaBinaryPackingValuesReader(reader);
            deltaBinaryPackingValuesReader.Init();
            return deltaBinaryPackingValuesReader;
        }

        private void Init() {
            _config = DeltaBinaryPackingConfig.ReadConfig(_reader);
            _totalValueCount = _reader.ReadUnsignedVarInt();

            // allocate valuesBuffer
            int totalMiniBlockCount = (int)Math.Ceiling((double)_totalValueCount / _config.MiniBlockSizeInValues);
            //+ 1 because first value written to header is also stored in values buffer
            _valuesBuffer = new long[(totalMiniBlockCount * _config.MiniBlockSizeInValues) + 1];

            _bitWidths = new int[_config.MiniBlockNumInABlock];

            // read first value from header
            _valuesBuffer[_valuesBuffered++] = _reader.ReadZigZagVarLong();

            while(_valuesBuffered < _totalValueCount) {
                //values Buffered could be more than totalValueCount, since we flush on a mini block basis
                LoadNewBlockToBuffer();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int ReadInteger() => (int)ReadLong();

        private long ReadLong() {
            if(_valuesRead > _totalValueCount) {
                throw new Exception("DeltaBinaryPackingValuesReader: read all values");
            }

            return _valuesBuffer[_valuesRead++];
        }

        private void LoadNewBlockToBuffer() {
            long minDeltaInCurrentBlock = _reader.ReadZigZagVarLong();

            for(int l = 0; l < _config.MiniBlockNumInABlock; l++) {
                _bitWidths[l] = _reader.ReadByte();
            }

            int i;
            for(i = 0; i < _config.MiniBlockNumInABlock && _valuesBuffered < _totalValueCount; i++) {
                UnpackMiniBlock(_bitWidths[i]);
            }

            //calculate values from deltas unpacked for current block
            int valueUnpacked = i * _config.MiniBlockSizeInValues;
            for(int j = _valuesBuffered - valueUnpacked; j < _valuesBuffered; j++) {
                _valuesBuffer[j] += minDeltaInCurrentBlock + _valuesBuffer[j - 1];
            }
        }

        private void UnpackMiniBlock(int bitWidth) {
            for(int i = 0; i < _config.MiniBlockSizeInValues; i += 8) {
                byte[] bytes = _reader.ReadBytes(bitWidth);
                var packer = new Packer.Packer(bitWidth);
                packer.Unpack8Values(bytes, 0, _valuesBuffer, _valuesBuffered);
                _valuesBuffered += 8;
            }
        }
    }
}