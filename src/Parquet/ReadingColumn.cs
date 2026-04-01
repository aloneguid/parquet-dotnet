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
    private IMemoryOwner<int>? _dictionaryIndexes;
    private int _definedDataCount = 0;
    private readonly int _dictionaryIndexesOffset = 0;
    private int _definitionOffset = 0;

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

    public Span<int> DictionaryIndexes {
        get {
            if(_dictionaryIndexes == null) {
                _dictionaryIndexes = MemoryOwner<int>.Allocate(_values.Length);
            }

            return _dictionaryIndexes.Memory.Span;
        }
    }

    public void Dispose() {

    }
}
