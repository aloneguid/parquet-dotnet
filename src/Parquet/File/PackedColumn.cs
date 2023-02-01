using System;
using System.Buffers;
using Parquet.Data;
using Parquet.Encodings;

namespace Parquet.File {

    /// <summary>
    /// Represents column data packed into Parquet logical parts.
    /// </summary>
    class PackedColumn : IDisposable {

        private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

        // fields declared in write order

        private Array? _dictionary;
        private int[]? _dictionaryIndexes;

        private readonly int[]? _repetitionLevels;

        private int[]? _definitionLevels;

        private Array _plainData;
        private int _plainDataOffset;
        private int _plainDataCount;

        private readonly DataColumn _column;

        public PackedColumn(DataColumn column) {
            _column = column;
            _repetitionLevels = column.RepetitionLevels;
            _plainData = column.Data;
            _plainDataOffset = column.Offset;
            _plainDataCount = column.Count;
        }

        public bool HasDictionary => _dictionary != null;

        public Array? Dictionary => _dictionary;

        public bool HasRepetitionLevels => _repetitionLevels != null;

        public int[]? RepetitionLevels => _repetitionLevels;

        public bool HasDefinitionLevels => _definitionLevels != null;

        public int[]? DefinitionLevels => _definitionLevels;

        public int[]? GetDictionaryIndexes(out int length) {
            length = (int)(_column.Count - _column.Statistics.NullCount);
            return _dictionaryIndexes;
        }

        public Array? GetPlainData(out int offset, out int count) {
            offset = _plainDataOffset;
            count = _plainDataCount;
            return _plainData;
        }

        /// <summary>
        /// Sets statistics: null count
        /// </summary>
        public void Pack(int maxDefinitionLevel, bool useDictionaryEncoding, double dictionaryThreshold) {

            // 1. definition levels

            if(maxDefinitionLevel > 0) {
                int nullCount = _column.CalculateNullCount();
                _column.Statistics.NullCount = nullCount;

                // having exact null count we can allocate/rent just the right buffer
                Array packedData = _column.Field.CreateArray(_column.Count - nullCount);
                _definitionLevels = IntPool.Rent(_column.Count);

                _column.PackDefinitions(_definitionLevels.AsSpan(0, _column.Count),
                    _plainData, _plainDataOffset, _plainDataCount,
                    packedData,
                    maxDefinitionLevel);

                _plainData = packedData;
                _plainDataOffset = 0;
                _plainDataCount = _column.Count - nullCount;
            } else {
                _column.Statistics.NullCount = 0;
            }

            // 2. try to extract dictionary

            if(useDictionaryEncoding && 
                // for some reason some readers do NOT understand dictionary-encoded arrays, but lists or plain columns are just fine
                !_column.Field.IsArray &&
                ParquetDictionaryEncoder.TryExtractDictionary(_column.Field.ClrType,
                    _plainData, _plainDataOffset, _plainDataCount,
                    out _dictionary, out _dictionaryIndexes, dictionaryThreshold)) {

                // if dictionary is successfully extracted, plainData is invalid
                _plainData = Array.Empty<string>();
                _plainDataOffset = 0;
                _plainDataCount = 0;

                _column.Statistics.DistinctCount = _dictionary!.Length;

                // note that dictionary indexes are pooled!
            }
        }

        public void Dispose() {
            if(_definitionLevels != null) {
                IntPool.Return(_definitionLevels);
            }

            if(_dictionaryIndexes != null) {
                IntPool.Return(_dictionaryIndexes);
            }
        }
    }
}