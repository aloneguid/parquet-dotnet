using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data;

/// <summary>
/// </summary>
/// <typeparam name="T"></typeparam>
public class RawColumnData<T> : IDisposable where T : struct {
    private readonly IMemoryOwner<T> _values;
    private readonly IMemoryOwner<int>? _definitionLevels;
    private readonly IMemoryOwner<int>? _repetitionLevels;

    /// <summary>
    /// </summary>
    /// <param name="values"></param>
    /// <param name="definitionLevels"></param>
    /// <param name="repetitionLevels"></param>
    internal RawColumnData(IMemoryOwner<T> values, IMemoryOwner<int>? definitionLevels, IMemoryOwner<int>? repetitionLevels) {
        _values = values;
        _definitionLevels = definitionLevels;
        _repetitionLevels = repetitionLevels;
    }

    /// <summary>
    /// </summary>
    public Span<T> Values => _values.Memory.Span;

    internal IEnumerable<T?> GetNullableValues() {

        int iv = 0;
        for(int id = 0; id < DefinitionLevels.Length; id++) {
            if(DefinitionLevels[id] == 0) {
                yield return null;
            } else {
                yield return Values[iv++];
            }
        }

        yield break;
    }

    /// <summary>
    /// Definition levels, if they exist. Otherwise, <see cref="InvalidOperationException"/> is thrown.
    /// </summary>
    public Span<int> DefinitionLevels {
        get {
            if(_definitionLevels == null)
                throw new InvalidOperationException("definition levels are not present for this column");
            return _definitionLevels.Memory.Span;
        }
    }

    /// <summary>
    /// Repetition levels, if they exist. Otherwise, <see cref="InvalidOperationException"/> is thrown.
    /// </summary>
    public Span<int> RepetitionLevels {
        get {
            if(_repetitionLevels == null)
                throw new InvalidOperationException("repetition levels are not present for this column");
            return _repetitionLevels.Memory.Span;
        }
    }

    internal List<string> ValuesAsStrings {
        get {
            if(typeof(T) != typeof(ReadOnlyMemory<char>))
                throw new InvalidOperationException("values are not strings");
            var r = new List<string>();
            foreach(ReadOnlyMemory<char> v in _values.Memory.Span.AsSpan<T, ReadOnlyMemory<char>>()) {
                r.Add(new string(v.Span));
            }
            return r;
        }
    }

    /// <summary>
    /// </summary>
    public void Dispose() {
        _values.Dispose();
        _definitionLevels?.Dispose();
        _repetitionLevels?.Dispose();
    }
}
