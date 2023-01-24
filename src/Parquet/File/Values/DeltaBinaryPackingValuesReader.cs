using System;
using System.IO;
using Parquet.Extensions;

namespace Parquet.File.Values {
    /// <summary>
    /// 
    /// </summary>
    public class DeltaBinaryPackingValuesReader {
        private readonly Stream _stream;
        private DeltaBinaryPackingConfig? _config;
        private int _totalValueCount;
        private long[]? _valuesBuffer;
        private int _valuesBuffered = 0;
        private int[]? _bitWidths;
        private int _valuesRead = 0;

        private DeltaBinaryPackingValuesReader(Stream s) => _stream = s;

        /// <summary>
        /// Gets and Initializes the configuration for the reader
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DeltaBinaryPackingValuesReader GetDeltaBinaryPackingValuesReader(Stream s) {
            var deltaBinaryPackingValuesReader = new DeltaBinaryPackingValuesReader(s);
            deltaBinaryPackingValuesReader.Init();
            return deltaBinaryPackingValuesReader;
        }

        private void Init() {
            _config = DeltaBinaryPackingConfig.ReadConfig(_stream);
            _totalValueCount = _stream.ReadUnsignedVarInt();

            // allocate valuesBuffer
            int totalMiniBlockCount = (int)Math.Ceiling((double)_totalValueCount / _config.MiniBlockSizeInValues);
            //+ 1 because first value written to header is also stored in values buffer
            _valuesBuffer = new long[(totalMiniBlockCount * _config.MiniBlockSizeInValues) + 1];

            _bitWidths = new int[_config.MiniBlockNumInABlock];

            // read first value from header
            _valuesBuffer[_valuesBuffered++] = _stream.ReadZigZagVarLong();

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
            if(_valuesBuffer == null)
                throw new InvalidOperationException("not initialised");

            if(_valuesRead > _totalValueCount) {
                throw new Exception("DeltaBinaryPackingValuesReader: read all values");
            }

            return _valuesBuffer[_valuesRead++];
        }

        private void LoadNewBlockToBuffer() {

            if(_config == null || _bitWidths == null || _valuesBuffer == null)
                throw new InvalidOperationException("not initialised");

            long minDeltaInCurrentBlock = _stream.ReadZigZagVarLong();

            for(int l = 0; l < _config.MiniBlockNumInABlock; l++) {
                _bitWidths[l] = _stream.ReadByte();
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

            if(_config == null || _valuesBuffer == null)
                throw new InvalidOperationException("not initialised");

            for(int i = 0; i < _config.MiniBlockSizeInValues; i += 8) {
                byte[] bytes = _stream.ReadBytesExactly(bitWidth);
                var packer = new Packer.Packer(bitWidth);
                packer.Unpack8Values(bytes, _valuesBuffer, _valuesBuffered);
                _valuesBuffered += 8;
            }
        }
    }
}