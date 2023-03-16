using System;
using System.Buffers;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.File {

    /// <summary>
    /// Represents column data packed into Parquet logical parts.
    /// </summary>
    class PackedColumn : IDisposable {

        private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

        // fields declared in write order

        private readonly DataField _field;

        private Array? _dictionary;
        private int[]? _dictionaryIndexes;
        private int _dictionaryIndexesOffset;

        private int[]? _repetitionLevels;
        private bool _repetitionsRented = false;
        private int _repetitionOffset = 0;

        private int[]? _definitionLevels;
        private int _definitionOffset = 0;

        private Array _plainData;
        private int _plainDataOffset;
        private int _plainDataCount;

        private readonly DataColumn? _column;

        public PackedColumn(DataColumn column) {
            _field = column.Field;
            _column = column;
            _repetitionLevels = column.RepetitionLevels;
            _plainData = column.Data;
            _plainDataOffset = column.Offset;
            _plainDataCount = column.Count;
        }

        public PackedColumn(DataField df, int valueCountIncludingNulls) {
            _field = df;
            _plainData = df.CreateArray(valueCountIncludingNulls);
        }

        public bool HasDictionary => _dictionary != null;

        public Array? Dictionary => _dictionary;

        public bool HasRepetitionLevels => _repetitionLevels != null;

        public int[]? RepetitionLevels => _repetitionLevels;

        public bool HasDefinitionLevels => _definitionLevels != null;

        public int[]? DefinitionLevels => _definitionLevels;

        public int[]? GetDictionaryIndexes(out int length) {
            length = (int)(_column!.Count - _column.Statistics.NullCount);
            return _dictionaryIndexes;
        }

        public Span<int> AllocateOrGetDictionaryIndexes(int max) {

            if(_dictionaryIndexes != null && _dictionaryIndexes.Length < max) {
                IntPool.Return(_dictionaryIndexes);
                _dictionaryIndexes = null;
            }

            if(_dictionaryIndexes == null)
                _dictionaryIndexes = IntPool.Rent(max);

            return _dictionaryIndexes.AsSpan(_dictionaryIndexesOffset);
        }

        public void MarkUsefulDictionaryIndexes(int count) {
            _dictionaryIndexesOffset += count;
        }

        public Span<int> GetWriteableRepetitionLevelSpan() {
            if(_repetitionLevels == null) {
                _repetitionLevels = IntPool.Rent(_plainData.Length + 8);
                _repetitionsRented = true;
            }

            return _repetitionLevels.AsSpan(_repetitionOffset);
        }

        public void MarkRepetitionLevels(int count) {
            _repetitionOffset += count;
        }

        public Span<int> GetWriteableDefinitionLevelSpan() {
            if(_definitionLevels == null) {
                _definitionLevels = IntPool.Rent(_plainData.Length + 8);
            }

            return _definitionLevels.AsSpan(_definitionOffset);
        }

        public void MarkDefinitionLevels(int count, int calculateNullLevel, out int nullCount) {
            nullCount = 0;
            if(calculateNullLevel != -1) {
                foreach(int level in _definitionLevels.AsSpan(_definitionOffset, count)) {
                    if(level != calculateNullLevel) {
                        nullCount++;
                    }
                }
            } 
            _definitionOffset += count;
        }

        public int DefinitionsRead => _definitionOffset;

        public Array GetPlainData(out int offset, out int count) {
            offset = _plainDataOffset;
            count = _plainDataCount;
            return _plainData;
        }

        public Array GetPlainDataToReadInto(out int offset) {
            offset = _plainDataCount;
            return _plainData;
        }

        public void MarkUsefulPlainData(int count) {
            _plainDataCount += count;
        }

        public void AssignDictionary(Array dictionary) {
            _dictionary = dictionary;
        }

        public int ValuesRead => HasDefinitionLevels 
            ? _definitionOffset 
            : (_dictionaryIndexes != null ? _dictionaryIndexesOffset : _plainDataCount);

        /// <summary>
        /// Sets statistics: null count
        /// </summary>
        public void Pack(int maxDefinitionLevel, bool useDictionaryEncoding, double dictionaryThreshold) {

            // 1. definition levels

            if(maxDefinitionLevel > 0) {
                int nullCount = _column!.CalculateNullCount();
                _column.Statistics.NullCount = nullCount;

                // having exact null count we can allocate/rent just the right buffer
                Array packedData = _column.Field.CreateArray(_column.Count - nullCount);
                _definitionLevels = IntPool.Rent(_column.Count);

                _column.PackDefinitions(_definitionLevels.AsSpan(0, _column.Count),
                    _plainData!, _plainDataOffset, _plainDataCount,
                    packedData,
                    maxDefinitionLevel);

                _plainData = packedData;
                _plainDataOffset = 0;
                _plainDataCount = _column.Count - nullCount;
            } else {
                _column!.Statistics.NullCount = 0;
            }

            // 2. try to extract dictionary

            if(useDictionaryEncoding && 
                // for some reason some readers do NOT understand dictionary-encoded arrays, but lists or plain columns are just fine
                !_column.Field.IsArray &&
                ParquetDictionaryEncoder.TryExtractDictionary(_column.Field.ClrType,
                    _plainData!, _plainDataOffset, _plainDataCount,
                    out _dictionary, out _dictionaryIndexes, dictionaryThreshold)) {

                // if dictionary is successfully extracted, plainData is invalid
                _plainData = Array.Empty<string>();
                _plainDataOffset = 0;
                _plainDataCount = 0;

                _column.Statistics.DistinctCount = _dictionary!.Length;

                // note that dictionary indexes are pooled!
            }
        }

        public void UnpackCheckpoint() {

            if(_dictionaryIndexes != null && _dictionary != null) {
                _dictionary.ExplodeFast(
                    _dictionaryIndexes.AsSpan(0, _dictionaryIndexesOffset),
                    _plainData, _plainDataCount, _dictionaryIndexesOffset);
                _plainDataCount += _dictionaryIndexesOffset;

                IntPool.Return(_dictionaryIndexes);
                _dictionaryIndexes = null;
                _dictionaryIndexesOffset = 0;
            }
        }

        public DataColumn Unpack(bool unpackDefinitions) {

            UnpackCheckpoint();

            if(_plainData == null)
                throw new InvalidOperationException("no plain data");

            return new DataColumn(_field, _plainData,
                DefinitionLevels == null ? null : DefinitionLevels.AsSpan(0,  _definitionOffset).ToArray(),
                RepetitionLevels == null ? null : RepetitionLevels.AsSpan(0, _repetitionOffset).ToArray(),
                unpackDefinitions);
        }

        public void Dispose() {

            if(_repetitionLevels != null && _repetitionsRented) {
                IntPool.Return(_repetitionLevels);
            }

            if(_definitionLevels != null) {
                IntPool.Return(_definitionLevels);
            }

            if(_dictionaryIndexes != null) {
                IntPool.Return(_dictionaryIndexes);
            }
        }
    }
}