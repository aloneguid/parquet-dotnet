using System;
using System.Buffers;
using System.Collections.Generic;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Column that's being written. This class holds required data parts, including allocation of any extra buffers (they will be disposed of when this class is disposed).
/// </summary>
class WritingColumn<T> : IDisposable where T : struct {
    private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;
    private static readonly ArrayPool<T> TPool = ArrayPool<T>.Shared;

    private readonly T[]? _valuesBase;              // if values are owned, this is the array from pool, otherwise null. Only here to return it to pool if we own it.
    private readonly ReadOnlyMemory<T> _values;     // may be owned
    private readonly int[]? _definitionLevels;      // always owned
    private readonly ReadOnlyMemory<int>? _repetitionLevels;    // never owned

    public static WritingColumn<T> NewWritingColumn(DataField field, ReadOnlyMemory<T> values, ReadOnlyMemory<int>? repetitionLevels) {
        Validate(field, repetitionLevels);

        return new WritingColumn<T>(field, values.Length, values, null, null, repetitionLevels);
    }

    public static WritingColumn<T> NewWritingColumn(DataField field, ReadOnlyMemory<T?> nullableValues, ReadOnlyMemory<int>? repetitionLevels) {

        Validate(field, repetitionLevels);

        // Calculate number of nulls beforehand in a separate cycle in order to consume some memory (we know how much to allocate after that).
        // We can also build definition levels in the same cycle.
        int[] definitionLevels = IntPool.Rent(nullableValues.Length);
        Span<int> span = definitionLevels.AsSpan(0, nullableValues.Length);
        int nullCount = FillDefinionsAndCountNulls(field, nullableValues.Span, ref span);

        // fill non-nulls
        int valueCount = nullableValues.Length - nullCount;
        T[] values = TPool.Rent(valueCount);
        FillNonNullValues(nullableValues.Span, values);

        return new WritingColumn<T>(field, nullableValues.Length, values, values, definitionLevels, repetitionLevels);
    }

    private static int FillDefinionsAndCountNulls(DataField df, ReadOnlySpan<T?> span, ref Span<int> definitionLevels) {
        int defIdx = 0;
        int count = 0;
        int vMissing = df.MaxDefinitionLevel - 1;
        int vPresent = df.MaxDefinitionLevel;
        foreach(T? t in span) {
            if(t == null) {
                definitionLevels[defIdx++] = vMissing;
                count++;
            } else {
                definitionLevels[defIdx++] = vPresent;
            }
        }
        return count;
    }

    private static void FillNonNullValues(ReadOnlySpan<T?> nullableValues, Span<T> values) {
        int valIdx = 0;
        foreach(T? t in nullableValues) {
            if(t != null) {
                values[valIdx++] = t.GetValueOrDefault();
            }
        }
    }

    private static void Validate(DataField field, ReadOnlyMemory<int>? repetitionLevels) {
        if(field is null)
            throw new ArgumentNullException(nameof(field));
        field.EnsureAttachedToSchema(nameof(field));

        bool typeCompatible =
            field.ClrType == typeof(T) ||
            (field.ClrType == typeof(string) && typeof(T) == typeof(ReadOnlyMemory<char>));

        if(!typeCompatible) {
            throw new ArgumentException($"expected values of type {field.ClrType} but passed {typeof(T)}");
        }

        if(field.MaxRepetitionLevel > 0) {
            if(repetitionLevels == null)
                throw new ArgumentException($"repetition levels are required for this field (RL={field.MaxRepetitionLevel})", nameof(repetitionLevels));
        }

    }

    private WritingColumn(DataField field,
        int numValues,
        ReadOnlyMemory<T> values, T[]? valuesBase,
        int[]? definitionLevels,
        ReadOnlyMemory<int>? repetitionLevels) {

        NumValues = numValues;
        Field = field;
        _values = values;
        _valuesBase = valuesBase;
        _definitionLevels = definitionLevels;
        _repetitionLevels = repetitionLevels;
    }

    public int NumValues { get; }

    public ReadOnlySpan<T> Values => _values.Span;

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

    public ReadOnlySpan<int> RepetitionLevels {
        get {
            if(!HasRepetitionLevels)
                throw new InvalidOperationException("This column doesn't have repetition levels");
            if(_repetitionLevels == null)
                throw new InvalidOperationException("Repetition levels are not ready yet");
            return _repetitionLevels.Value.Span;
        }
    }

    public ReadOnlySpan<int> DefinitionLevels {
        get {
            if(!HasDefinitionLevels)
                throw new InvalidOperationException("This column doesn't have definition levels");
            if(_definitionLevels == null)
                throw new InvalidOperationException("Definition levels are not ready yet");

            return _definitionLevels.AsSpan(0, NumValues);
        }
    }

    public DataColumnStatistics Statistics { get; internal set; } =
        new DataColumnStatistics(null, null, null, null);

    public void Pack(bool useDictionaryEncoding, double dictionaryThreshold) {

    }

    public void Dispose() {
        if(_definitionLevels != null) {
            IntPool.Return(_definitionLevels);
        }

        if(_valuesBase !=null) {
            TPool.Return(_valuesBase);
        }

    }
}
