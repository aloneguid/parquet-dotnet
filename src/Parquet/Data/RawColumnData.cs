using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data;

/// <summary>
/// Base interface for raw column data, to hold something other than raw object.
/// </summary>
public abstract class RawColumnData : IDisposable {

    private readonly IMemoryOwner<int>? _definitionLevels;
    private readonly IMemoryOwner<int>? _repetitionLevels;

    /// <summary>
    /// Initializes a new instance of the RawColumnData class with the specified definition and repetition levels.
    /// </summary>
    /// <param name="definitionLevels">
    /// An optional memory owner containing the definition levels for the column data. May be null if definition levels
    /// are not required.
    /// </param>
    /// <param name="repetitionLevels">
    /// An optional memory owner containing the repetition levels for the column data. May be null if repetition levels
    /// are not required.
    /// </param>
    protected RawColumnData(IMemoryOwner<int>? definitionLevels, IMemoryOwner<int>? repetitionLevels) {
        _definitionLevels = definitionLevels;
        _repetitionLevels = repetitionLevels;
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

    internal ReadOnlyMemory<int>? DefinitionLevelsMemoryOrNull => _definitionLevels?.Memory;

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

    internal ReadOnlyMemory<int>? RepetitionLevelsMemoryOrNull => _repetitionLevels?.Memory;

    /// <inheritdoc/>
    public virtual void Dispose() {
        _definitionLevels?.Dispose();
        _repetitionLevels?.Dispose();
    }
}

/// <summary>
/// Used as a container for column data read if you don't want to allocate the memory yourself.
/// </summary>
/// <typeparam name="T"></typeparam>
public class RawColumnData<T> : RawColumnData where T : struct {
    private readonly IMemoryOwner<T> _values;

    /// <summary>
    /// Initializes a new instance of the RawColumnData class with the specified values, definition levels, and
    /// repetition levels.
    /// </summary>
    /// <param name="values">An memory owner containing the values for the column data.</param>
    /// <param name="definitionLevels">
    /// An optional memory owner containing the definition levels for the column data. May be null if definition levels
    /// are not required.
    /// </param>
    /// <param name="repetitionLevels">
    /// An optional memory owner containing the repetition levels for the column data. May be null if repetition levels
    /// are not required.
    /// </param>
    internal RawColumnData(IMemoryOwner<T> values, IMemoryOwner<int>? definitionLevels, IMemoryOwner<int>? repetitionLevels) : base(definitionLevels, repetitionLevels) {
        _values = values;
    }

    /// <summary>
    /// </summary>
    public Span<T> Values => _values.Memory.Span;

    internal ReadOnlyMemory<T> ValuesMemory => _values.Memory;

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

    /// <inheritdoc/>
    public override void Dispose() {
        base.Dispose();
        _values.Dispose();
    }
}
