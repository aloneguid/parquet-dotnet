using System;
using System.IO;

namespace Parquet.File.Values
{
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        public DeltaBinaryPackingValuesReader(BinaryReader reader) {
            _reader = reader;
        }

        /// <summary>
        /// 
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
            int totalMiniBlockCount = (int) Math.Ceiling((double) _totalValueCount / _config.MiniBlockSizeInValues);
            //+ 1 because first value written to header is also stored in values buffer
            _valuesBuffer = new long[(totalMiniBlockCount * _config.MiniBlockSizeInValues) + 1];
            
            _bitWidths = new int[_config.MiniBlockNumInABlock];
            
            //read first value from header
            _valuesBuffer[_valuesBuffered++] = _reader.ReadZigZagVarLong();
            
            while (_valuesBuffered < _totalValueCount) { //values Buffered could be more than totalValueCount, since we flush on a mini block basis
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
            
            for (int l = 0; l < _config.MiniBlockNumInABlock; l++) {
                _bitWidths[l] = _reader.ReadByte();
            }

            int i;
            for (i = 0; i < _config.MiniBlockNumInABlock && _valuesBuffered < _totalValueCount; i++) {
                UnpackMiniBlock(_bitWidths[i]);
            }

            //calculate values from deltas unpacked for current block
            int valueUnpacked = i * _config.MiniBlockSizeInValues;
            for (int j = _valuesBuffered-valueUnpacked; j < _valuesBuffered; j++) {
                int index = j;
                _valuesBuffer[index] += minDeltaInCurrentBlock + _valuesBuffer[index - 1];
            }
            
        }

        private void UnpackMiniBlock(int bitWidth) {
            for (int i = 0; i < _config.MiniBlockSizeInValues; i += 8) {
                int bytesToRead = bitWidth * 8;
                byte[] bytes = _reader.ReadBytes(bytesToRead);
                
                for(int j = 0; j < 8; j++) {
                    // TODO: bitWidth to long
                    //_valuesBuffer[_valuesBuffered++] = ValuesUtils.ReadIntOnBytes(bytes);
                }
            }
        }
    }
}