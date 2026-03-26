using System;
using System.Buffers;
using System.Collections.Generic;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Column that's being written. This class holds required data parts, including allocation of any extra buffers (they will be disposed of when this class is disposed).
/// </summary>
class WritingColumn<T> : IDisposable {
    private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

    private readonly ReadOnlyMemory<T> _values;
    private object? _nonNullValues = null;
    private int _nonNullValueCount;
    private bool _ownsNonNullValues = false;
    private int[]? _definitionLevels = null;
    private readonly ReadOnlyMemory<int>? _repetitionLevels;


    /// <summary>
    /// 
    /// </summary>
    /// <param name="field"></param>
    /// <param name="values">When writing nullable field, the values should be of the nullable type. Definition levels will be calculated internally.</param>
    /// <param name="repetitionLevels"></param>
    /// <exception cref="ArgumentNullException"></exception>
    public WritingColumn(DataField field,
        ReadOnlyMemory<T> values,
        ReadOnlyMemory<int>? repetitionLevels) {

        if(field is null)
            throw new ArgumentNullException(nameof(field));
        field.EnsureAttachedToSchema(nameof(field));

        if(field.ClrNullableIfHasNullsType != typeof(T)) {
            throw new ArgumentException($"expected values of type {field.ClrNullableIfHasNullsType} but passed {typeof(T)}", nameof(values));
        }

        if(field.MaxRepetitionLevel > 0) {
            if(repetitionLevels == null)
                throw new ArgumentException($"repetition levels are required for this field (RL={field.MaxRepetitionLevel})", nameof(repetitionLevels));
        }

        Field = field;
        _values = values;
        _repetitionLevels = repetitionLevels;

        ForkToDataAndDefinitionLevels();
    }

    public int NumValues => _values.Length;

    public bool HasDictionary => false;

    public bool HasDefinitionLevels => Field.MaxDefinitionLevel > 0;

    public bool HasRepetitionLevels => Field.MaxRepetitionLevel > 0;

    internal long CalculateRowCount() {

        if(Field.MaxRepetitionLevel > 0) {
            return _repetitionLevels!.Value.Span.Count(0);
        }

        return NumValues;
    }

    public DataField Field { get; }

    /// <summary>
    /// Returns <see cref="ReadOnlyMemory{T}"/> where T is the underlying non-nullable type.
    /// </summary>
    public object PackedValuesReadOnlyMemory {
        get {
            if(_nonNullValues == null)
                throw new InvalidOperationException("Packed values are not ready yet");

            if(_ownsNonNullValues) {
                // _nonNullValues is Array type with elements of type T (non-null).
                // we need to case it to ROM<T>
                Type elementType = Field.ClrType;
                return elementType.CreateReadOnlyMemory((Array)_nonNullValues, 0, _nonNullValueCount);
            }

            return _nonNullValues;
        }
    }

    public Span<int> DefinitionLevels {
        get {
            if(!HasDefinitionLevels)
                throw new InvalidOperationException("This column doesn't have definition levels");
            if(_definitionLevels == null)
                throw new InvalidOperationException("Definition levels are not ready yet");

            return _definitionLevels.AsSpan(0, _values.Length);
        }
    }

    public DataColumnStatistics Statistics { get; internal set; } =
        new DataColumnStatistics(null, null, null, null);

    private void ForkToDataAndDefinitionLevels() {
        if(Field.MaxDefinitionLevel == 0) {
            _nonNullValues = _values;
            _definitionLevels = null;
            return;
        }

        // If we are here, the values are nullable, so we have to pack them.
        // Even if no values have nulls in them, we still need to move nullable values to nun-nullable types. That's not just for pedantic reasons, but it greatly helps encoding and compression.
        _definitionLevels = IntPool.Rent(_values.Length);
        int nullCount = FillDefinionsAndCountNulls(_values.Span);

        _nonNullValueCount = _values.Length - nullCount;
        _nonNullValues = Field.ClrType.RentArrayFromPool(_nonNullValueCount);
        _ownsNonNullValues = true;

        //

        throw new NotImplementedException();
    }

    /// <summary>
    /// Values should be packed into non-nullable _nonNullValues and definition levels.
    /// </summary>
    private void Pack() {
        for(int iV = 0, iNNV = 0; iV < _values.Length; iV++) {
            bool isNull = _definitionLevels![iV] != Field.MaxDefinitionLevel;
            if(!isNull) {
                ((Array)_nonNullValues).set
            }
        }
    }

    private int FillDefinionsAndCountNulls(ReadOnlySpan<T> span) {
        if(_definitionLevels == null)
            throw new InvalidOperationException("Definition levels buffer is not allocated");

        int defIdx = 0;
        int count = 0;
        EqualityComparer<T> comp = EqualityComparer<T>.Default;
        int vMissing = Field.MaxDefinitionLevel - 1;
        int vPresent = Field.MaxDefinitionLevel;
        foreach(T t in span) {
            if(comp.Equals(t, default(T))) {
                _definitionLevels[defIdx++] = vMissing;
                count++;
            } else {
                _definitionLevels[defIdx++] = vPresent;
            }
        }
        return count;
    }

    public void Dispose() {
        if(_definitionLevels != null) {
            IntPool.Return(_definitionLevels);
            _definitionLevels = null;
        }

        if(_nonNullValues != null && _ownsNonNullValues) {
            ((Array)_nonNullValues).ReturnArrayToPool();
            _nonNullValues = null;
        }
    }
}
