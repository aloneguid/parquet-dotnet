using System;
using System.Buffers;
using System.Drawing;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.File {

    /// <summary>
    /// Represents column data packed into Parquet logical parts. This is an intermediate data structure that
    /// will be incorporated into <see cref="DataColumn"/> when the times are better.
    /// </summary>
    class PackedColumn : IDisposable {

        private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

        // fields declared in write order

        private readonly DataField _field;
        private readonly int _totalCount;
        private Array? _dictionary;
        private int[]? _dictionaryIndexes;
        private int _dictionaryIndexesOffset;

        private int[]? _repetitionLevels;
        private bool _repetitionsRented = false;
        private int _repetitionOffset = 0;

        private int[]? _definitionLevels;
        private int _definitionOffset = 0;
        private bool _areDefinitionsPooled;

        private Array _definedData;
        private int _definedDataCount;

        private readonly DataColumn? _column;

        public PackedColumn(DataColumn column) {
            _field = column.Field;
            _column = column;
            _definitionLevels = column.DefinitionLevels;
            _repetitionLevels = column.RepetitionLevels;
            _definedData = column.DefinedData;
            _definedDataCount = column.DefinedData.Length;
        }

        public PackedColumn(DataField df, int totalCount, int approxDefinedCount) {
            _field = df;
            _totalCount = totalCount;
            _definedData = df.CreateArray(approxDefinedCount);
        }

        public bool HasDictionary => _dictionary != null;

        public Array? Dictionary => _dictionary;

        public bool HasRepetitionLevels => _repetitionLevels != null;

        public int[]? RepetitionLevels => _repetitionLevels;

        public bool HasDefinitionLevels => _definitionLevels != null;

        public int[]? DefinitionLevels => _definitionLevels;

        public int[]? GetDictionaryIndexes(out int length) {
            length = (int)(_column!.NumValues - _column.Statistics.NullCount ?? 0);
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
                _repetitionLevels = IntPool.Rent(_totalCount + 8);
                _repetitionsRented = true;
            }

            return _repetitionLevels.AsSpan(_repetitionOffset);
        }

        public void MarkRepetitionLevels(int count) {
            _repetitionOffset += count;
        }

        public Span<int> GetWriteableDefinitionLevelSpan() {
            _definitionLevels ??= IntPool.Rent(_totalCount + 8);
            _areDefinitionsPooled = true;

            return _definitionLevels.AsSpan(_definitionOffset);
        }

        public void MarkDefinitionLevels(int count) {
            _definitionOffset += count;
        }

        public int MarkDefinitionLevels(int count, int dl) {
            int nullCount = 0;
            foreach(int level in _definitionLevels.AsSpan(_definitionOffset, count)) {
                if(level != dl) {
                    nullCount++;
                }
            }

            _definitionOffset += count;
            return nullCount;
        }

        public int DefinitionsRead => _definitionOffset;

        public Array GetPlainData(out int offset, out int count) {
            offset = 0;
            count = _definedDataCount;
            return _definedData;
        }

        public Array GetPlainDataToReadInto(out int offset) {
            offset = _definedDataCount;
            return _definedData;
        }

        public void MarkUsefulPlainData(int count) {
            _definedDataCount += count;
        }

        public void AssignDictionary(Array dictionary) {
            _dictionary = dictionary;
        }

        public int ValuesRead => HasDefinitionLevels 
            ? _definitionOffset 
            : (_dictionaryIndexes != null ? _dictionaryIndexesOffset : _definedDataCount);

        /// <summary>
        /// Sets statistics: null count
        /// </summary>
        public void Pack(bool useDictionaryEncoding, double dictionaryThreshold) {

            if(_column == null)
                throw new NullReferenceException();

            // try to extract dictionary

            if(useDictionaryEncoding && 
                // for some reason some readers do NOT understand dictionary-encoded arrays, but lists or plain columns are just fine
                !_column.Field.IsArray &&
                ParquetDictionaryEncoder.TryExtractDictionary(_column.Field.ClrType,
                    _definedData!, 0, _definedDataCount,
                    out _dictionary, out _dictionaryIndexes, dictionaryThreshold)) {

                // if dictionary is successfully extracted, plainData is invalid
                _definedData = Array.Empty<string>();
                _definedDataCount = 0;

                _column.Statistics.DistinctCount = _dictionary!.Length;

                // note that dictionary indexes are pooled!
            }
        }

        public void Checkpoint() {

            if(_dictionaryIndexes != null && _dictionary != null) {
                _dictionary.ExplodeFast(
                    _dictionaryIndexes.AsSpan(0, _dictionaryIndexesOffset),
                    _definedData, _definedDataCount, _dictionaryIndexesOffset);
                _definedDataCount += _dictionaryIndexesOffset;

                IntPool.Return(_dictionaryIndexes);
                _dictionaryIndexes = null;
                _dictionaryIndexesOffset = 0;
            }
        }

        public DataColumn Unpack() {

            Checkpoint();

            if(_definedData == null)
                throw new InvalidOperationException("no plain data");

            Array dcData;
            if(_definedDataCount != _definedData.Length) {
                dcData = _field.CreateArray(_definedDataCount);

                //If the arrays aren't the exact same size let's copy what fits
                int copyAmount = _definedDataCount > _definedData.Length 
                    ? _definedData.Length : _definedDataCount;
                Array.Copy(_definedData, dcData, copyAmount);
            } else {
                dcData = _definedData;
            }

            return new DataColumn(_field, dcData,
                DefinitionLevels?.AsSpan(0, _definitionOffset).ToArray(),
                RepetitionLevels?.AsSpan(0, _repetitionOffset).ToArray());
        }

        public void Dispose() {

            if(_repetitionLevels != null && _repetitionsRented) {
                IntPool.Return(_repetitionLevels);
            }

            if(_definitionLevels != null && _areDefinitionsPooled) {
                IntPool.Return(_definitionLevels);
            }

            if(_dictionaryIndexes != null) {
                IntPool.Return(_dictionaryIndexes);
            }
        }
    }
}