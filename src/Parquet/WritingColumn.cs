using System;
using System.Buffers;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Column that's being written. This class holds required data parts, including allocation of any extra buffers (they will be disposed of when this class is disposed).
/// </summary>
class WritingColumn<T> : IDisposable {
    private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

    private readonly ReadOnlyMemory<T> _values;
    private object? _packedValues = null;
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

    public object PackedValuesReadOnlyMemory {
        get {
            if(_packedValues == null)
                throw new InvalidOperationException("Packed values are not ready yet");
            return _packedValues;
        }
    }

    public Span<int> DefinitionLevels {
        get {
            if(!HasDefinitionLevels)
                throw new InvalidOperationException("This column doesn't have definition levels");
            if(_definitionLevels == null)
                throw new InvalidOperationException("Definition levels are not ready yet");
            return _definitionLevels;
        }
    }

    public DataColumnStatistics Statistics { get; internal set; } =
        new DataColumnStatistics(null, null, null, null);

    private void ForkToDataAndDefinitionLevels() {
        if(Field.MaxDefinitionLevel == 0) {
            _packedValues = _values;
            _definitionLevels = null;
            return;
        }

        // if we are here, the values are nullable, so we have to pack them

        throw new NotImplementedException();
    }

    public void Dispose() {

    }
}
