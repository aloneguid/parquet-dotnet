using System;
using System.Buffers;
using CommunityToolkit.HighPerformance.Buffers;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Column that's being written. This class holds required data parts, including allocation of any extra buffers (they
/// will be disposed of when this class is disposed).
/// </summary>
class WritingColumn<T> : IDisposable where T : struct {
    private readonly IMemoryOwner<T>? _valuesOwner;                 // not null if values are owned by this class
    private readonly ReadOnlyMemory<T> _values;                     // may be owned
    private readonly IMemoryOwner<int>? _definitionLevelsOwner;     // not null if definition levels are owned by this class
    private readonly ReadOnlyMemory<int>? _definitionLevels;        // may be owned
    private readonly ReadOnlyMemory<int>? _repetitionLevels;        // never owned
    private IMemoryOwner<T>? _dictionary;
    private IMemoryOwner<int>? _dictionaryIndexes;

    public static WritingColumn<T> NewWritingColumn(DataField field, ReadOnlyMemory<T> values, ReadOnlyMemory<int>? repetitionLevels) {
        Validate(field, repetitionLevels);

        return new WritingColumn<T>(field, values.Length, values, null, null, null, repetitionLevels);
    }

    public static WritingColumn<T> NewWritingColumn(DataField field, ReadOnlyMemory<T?> nullableValues, ReadOnlyMemory<int>? repetitionLevels) {

        Validate(field, repetitionLevels);

        // Calculate number of nulls beforehand in a separate cycle in order to consume some memory (we know how much to allocate after that).
        // We can also build definition levels in the same cycle.
        IMemoryOwner<int> definitionLevelsOwner = MemoryOwner<int>.Allocate(nullableValues.Length);
        Span<int> span = definitionLevelsOwner.Memory.Span;
        int nullCount = FillDefinionsAndCountNulls(field, nullableValues.Span, ref span);

        // fill non-nulls
        int valueCount = nullableValues.Length - nullCount;
        IMemoryOwner<T> valuesOwner = MemoryOwner<T>.Allocate(valueCount);
        FillNonNullValues(nullableValues.Span, valuesOwner.Memory.Span);

        return new WritingColumn<T>(field, nullableValues.Length, valuesOwner.Memory, valuesOwner, definitionLevelsOwner.Memory, definitionLevelsOwner, repetitionLevels);
    }

    public static WritingColumn<T> NewWritingColumn(DataField field, ReadOnlyMemory<T> values, ReadOnlyMemory<int>? definitionLevels, ReadOnlyMemory<int>? repetitionLevels) {
        Validate(field, repetitionLevels);
        return new WritingColumn<T>(field,
            definitionLevels?.Length ?? values.Length,
            values, null, definitionLevels, null, repetitionLevels);
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
            (field.ClrType == typeof(string) && typeof(T) == typeof(ReadOnlyMemory<char>)) ||
            (field.ClrType == typeof(byte[]) && typeof(T) == typeof(ReadOnlyMemory<byte>));

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
        ReadOnlyMemory<T> values, IMemoryOwner<T>? valuesOwner,
        ReadOnlyMemory<int>? definitionLevels, IMemoryOwner<int>? definitionLevelsOwner,
        ReadOnlyMemory<int>? repetitionLevels) {

        NumValues = numValues;
        Field = field;
        _values = values;
        _valuesOwner = valuesOwner;
        _definitionLevels = definitionLevels;
        _definitionLevelsOwner = definitionLevelsOwner;
        _repetitionLevels = repetitionLevels;

        if(field.IsArray && _definitionLevels == null) {
            // special use case for legacy arrays
            _definitionLevelsOwner = MemoryOwner<int>.Allocate(numValues);
            _definitionLevels = _definitionLevelsOwner.Memory;
            // fill all levels with MaxDefinitionLevel quickly
            int maxDefLevel = field.MaxDefinitionLevel;
            for(int i = 0; i < numValues; i++) {
                _definitionLevelsOwner.Memory.Span[i] = maxDefLevel;
            }
        }

        Statistics.NullCount = numValues - values.Length;
    }

    /// <summary>
    /// Total number of values in this column. If column is nullable, this includes nulls. If column is repeated, this
    /// includes all values in all repetitions.
    /// </summary>
    public int NumValues { get; }

    public ReadOnlySpan<T> Values => _values.Span;

    public bool HasDictionary => _dictionary != null;

    public Span<T> Dictionary {
        get {
            if(_dictionary == null) {
                throw new InvalidOperationException("This column doesn't have a dictionary");
            }
            return _dictionary.Memory.Span;
        }
    }

    public Span<int> DictionaryIndexes {
        get {
            if(_dictionaryIndexes == null) {
                throw new InvalidOperationException("This column doesn't have dictionary indexes");
            }

            return _dictionaryIndexes.Memory.Span;
        }
    }

    public bool HasDefinitionLevels => Field.MaxDefinitionLevel > 0;

    public bool HasRepetitionLevels => Field.MaxRepetitionLevel > 0;

    internal long CalculateRowCount() {

        if(Field.MaxRepetitionLevel > 0) {
            return _repetitionLevels!.Value.Span.Count(0);
        }

        return NumValues;
    }

    /// <summary>
    /// The schema field associated with this column.
    /// </summary>
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

            return _definitionLevels.Value.Span;
        }
    }

    public DataColumnStatistics Statistics { get; internal set; } =
        new DataColumnStatistics(null, null, null, null);

    public void Pack(ParquetOptions options) {
        // for some reason some readers do NOT understand dictionary-encoded arrays, but lists or plain columns are just fine
        if(Field.IsArray)
            return;

        // only support strings for now, this is the only type that make sense to dictionry-encode effectively
        if(typeof(T) != typeof(ReadOnlyMemory<char>))
            return;

        // only encode columns specified in options
        if(options.GetEncodingHint(Field) != EncodingHint.Dictionary)
            return;

        // cast ReadOnlyMemory<T> to ReadOnlyMemory<ReadOnlyMemory<char>>
        ReadOnlySpan<ReadOnlyMemory<char>> stringsSpan = Values.AsSpan<T, ReadOnlyMemory<char>>();
        if(ParquetDictionaryEncoder.TryExtractDictionary(stringsSpan, options.DictionaryEncodingThreshold,
            out IMemoryOwner<ReadOnlyMemory<char>>? dictionaryOwner,
            out _dictionaryIndexes)) {
            // case memory back to ReadOnlyMemory<T>
            _dictionary = dictionaryOwner as IMemoryOwner<T>;
            if(_dictionary == null) {
                dictionaryOwner.Dispose();
                throw new InvalidOperationException("Unexpected error during dictionary encoding: could not cast dictionary to expected type");
            }
        }
    }

    public void Dispose() {
        if(_definitionLevelsOwner != null) {
            _definitionLevelsOwner.Dispose();
        }

        if(_valuesOwner != null) {
            _valuesOwner.Dispose();
        }

        _dictionaryIndexes?.Dispose();
    }
}
