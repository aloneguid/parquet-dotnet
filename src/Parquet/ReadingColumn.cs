using System;
using System.Buffers;
using CommunityToolkit.HighPerformance.Buffers;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Just like <see cref="WritingColumn{T}"/>, contains intermediate data structures used during column reading.
/// Keps separate as trying to share code between reading and writing is more trouble than it's worth.
/// </summary>
class ReadingColumn<T> : IDisposable where T : struct {
    private readonly Memory<T> _values;
    private readonly Memory<int>? _definitionLevels;
    private readonly Memory<int>? _repetitionLevels;
    private IMemoryOwner<T>? _dictionary;
    private IMemoryOwner<int>? _dictionaryIndexes;
    private int _definedDataCount = 0;
    private int _dictionaryIndexesOffset = 0;
    private int _definitionOffset = 0;
    private int _repetitionOffset = 0;

    public ReadingColumn(DataField field, Memory<T> values, Memory<int>? definitionLevels, Memory<int>? repetitionLevels) {
        Field = field;
        _values = values;
        _definitionLevels = definitionLevels;
        _repetitionLevels = repetitionLevels;
    }

    public DataField Field { get; }

    public Span<T> Values => _values.Span;

    public Span<T> ValuesToReadInto => _values.Span.Slice(_definedDataCount);

    public Span<int> DefinitionLevelsToReadInto {
        get {
            if(_definitionLevels == null)
                throw new InvalidOperationException($"Definition levels buffer is not allocated for field '{Field.Path}'");
            return _definitionLevels.Value.Span.Slice(_definitionOffset);
        }
    }

    public Span<int> RepetitionLevelsToReadInto {
        get {
            if(_repetitionLevels == null)
                throw new InvalidOperationException($"Repetition levels buffer is not allocated for field '{Field.Path}'");
            return _repetitionLevels.Value.Span.Slice(_repetitionOffset);
        }
    }

    public int ValuesRead => Field.MaxDefinitionLevel > 0
        ? _definitionOffset
        : (_dictionaryIndexes != null ? _dictionaryIndexesOffset : _definedDataCount);

    public void MarkValuesRead(int count) {
        _definedDataCount += count;
    }

    public int MarkDefinitionLevels(int count, int dl) {
        if(_definitionLevels == null)
            throw new InvalidOperationException($"Definition levels buffer is not allocated for field '{Field.Path}'");

        int nullCount = 0;
        foreach(int level in _definitionLevels.Value.Span.Slice(_definitionOffset, count)) {
            if(level != dl) {
                nullCount++;
            }
        }

        _definitionOffset += count;
        return nullCount;
    }

    public void MarkRepetitionLevels(int count) {
        if(_repetitionLevels == null)
            throw new InvalidOperationException($"Repetition levels buffer is not allocated for field '{Field.Path}'");
        _repetitionOffset += count;
    }

    public Span<int> DictionaryIndexes {
        get {
            if(_dictionaryIndexes == null) {
                _dictionaryIndexes = MemoryOwner<int>.Allocate(_values.Length);
            }

            return _dictionaryIndexes.Memory.Span;
        }
    }

    public bool HasDictionary => _dictionary != null;

    public Span<T> AllocateDictionary(int count) {
        if(_dictionary != null)
            throw new InvalidOperationException($"Dictionary is already allocated for field '{Field.Path}'");

        _dictionary = MemoryOwner<T>.Allocate(count);
        return _dictionary.Memory.Span;
    }

    public Span<int> AllocateOrGetDictionaryIndexes(int max) {
        if(_dictionaryIndexes == null) {
            _dictionaryIndexes = MemoryOwner<int>.Allocate(max);
        }
        return _dictionaryIndexes.Memory.Span.Slice(_dictionaryIndexesOffset);
    }

    public void MarkDictionaryIndexesRead(int count) {
        _dictionaryIndexesOffset += count;
    }

    public void Checkpoint() {

        if(_dictionaryIndexes != null && _dictionary != null) {
            // explode dictionary and indexes into values memory
            int valueCount = _dictionaryIndexesOffset;
            Span<int> indexes = _dictionaryIndexes.Memory.Span.Slice(0, _dictionaryIndexesOffset);
            Span<T> dictionary = _dictionary.Memory.Span;

            for(int i = 0; i < valueCount; i++) {
                int index = indexes[i];
                _values.Span[_definedDataCount + i] = dictionary[index];
            }
            _definedDataCount = valueCount;

            // cleanup
            _dictionary.Dispose();
            _dictionary = null;
            _dictionaryIndexes.Dispose();
            _dictionaryIndexes = null;
            _dictionaryIndexesOffset = 0;
        }
    }


    public void Dispose() {
        _dictionary?.Dispose();
        _dictionaryIndexes?.Dispose();
    }
}
