using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance.Buffers;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;
using SType = System.Type;

namespace Parquet;

/// <summary>
/// Reader for Parquet row groups
/// </summary>
public class ParquetRowGroupReader : IDisposable {
    private readonly RowGroup _rowGroup;
    private readonly ThriftFooter _footer;
    private readonly Stream _stream;
    private readonly ParquetOptions? _options;
    private readonly Dictionary<FieldPath, ColumnChunk> _pathToChunk = new();

    internal ParquetRowGroupReader(
       RowGroup rowGroup,
       ThriftFooter footer,
       Stream stream,
       ParquetOptions? parquetOptions) {
        _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
        _footer = footer ?? throw new ArgumentNullException(nameof(footer));
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _options = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

        //cache chunks
        foreach(ColumnChunk hunk in _rowGroup.Columns) {
            FieldPath path = hunk.GetPath();
            _pathToChunk[path] = hunk;
        }
    }

    /// <summary>
    /// Exposes raw metadata about this row group
    /// </summary>
    public RowGroup RowGroup => _rowGroup;

    /// <summary>
    /// Gets the number of rows in this row group, which includes null values if any.
    /// </summary>
    /// <remarks>
    /// If this row group contains repeated columns, the number of values in those columns may be greater than the row
    /// count, but the row count will always reflect the number of rows, including nulls, in the source data.
    /// </remarks>
    public long RowCount => _rowGroup.NumRows;

    /// <summary>
    /// Checks if this field exists in source schema
    /// </summary>
    public bool ColumnExists(DataField field) {
        return GetMetadata(field) != null;
    }

    private ColumnMetaData GetColumnMetaData(DataField field) {
        ColumnChunk columnChunk = GetMetadata(field)
            ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
        if(columnChunk.MetaData == null)
            throw new ParquetException($"Column chunk metadata is missing for '{field.Path}', meaning the file is most probably corrupt");
        return columnChunk.MetaData;
    }

    /// <summary>
    /// Performs "raw" reading of a column, which returns non-nullable values and definition/repetition levels as
    /// separate buffers. Intended for advanced use only. todo: explain what to use instead
    /// </summary>
    public async ValueTask ReadRawAsync<T>(DataField field,
        Memory<T> values,
        Memory<int>? definitionLevels,
        Memory<int>? repetitionLevels,
        CancellationToken cancellationToken = default)
        where T : struct {


        if(!field.IsCompatibleWith(typeof(T))) {
            throw new ArgumentException($"Field \"{field.Path}\" ({field.ClrType}) is not compatible with type {typeof(T)}");
        }

        ColumnChunk columnChunk = GetMetadata(field)
            ?? throw new ParquetException($"'{field.Path}' does not exist in this file");

        long numValues = GetColumnMetaData(field).NumValues;

        // If column is nullable, the buffer does not have to be as big as numValuees, it's just the upper limit.
        // This can be slightly improved by trying to read statistics, however those can be unreliable.
        if(values.Length < numValues)
            throw new ArgumentException($"Values buffer is too small for field '{field.Path}' (length {values.Length} < numValues {numValues})");

        if(field.MaxDefinitionLevel > 0 && definitionLevels == null)
            throw new ArgumentException($"Definition levels buffer is required for field '{field.Path}' ({nameof(Field.MaxDefinitionLevel)} = {field.MaxDefinitionLevel})");

        if(field.MaxRepetitionLevel > 0 && repetitionLevels == null)
            throw new ArgumentException($"Repetition levels buffer is required for field '{field.Path}' ({nameof(Field.MaxRepetitionLevel)} = {field.MaxRepetitionLevel})");

        if(definitionLevels != null && definitionLevels.Value.Length < numValues)
            throw new ArgumentException($"Definition levels buffer is too small for field '{field.Path}' (length {definitionLevels.Value.Length} < numValues {RowCount})");

        if(repetitionLevels != null && repetitionLevels.Value.Length < numValues)
            throw new ArgumentException($"Repetition levels buffer is too small for field '{field.Path}' (length {repetitionLevels.Value.Length} < numValues {RowCount})");

        var columnReader = new DataColumnReader(field, _stream,
            columnChunk, ReadColumnStatistics(columnChunk), _footer, _options);

        using var rc = new ReadingColumn<T>(field, values, definitionLevels, repetitionLevels);
        await columnReader.ReadAsync(rc, cancellationToken);
    }

    /// <summary>
    /// Pre-allocates column memory and reads.
    /// </summary>
    internal async ValueTask<RawColumnData<T>> ReadRawColumnDataAsync<T>(DataField field,
       CancellationToken cancellationToken = default)
       where T : struct {

        // allocate required buffers
        long numValues = GetColumnMetaData(field).NumValues;
        IMemoryOwner<T> values = MemoryOwner<T>.Allocate((int)numValues);
        IMemoryOwner<int>? definitionLevels = field.MaxDefinitionLevel > 0 ? MemoryOwner<int>.Allocate((int)numValues) : null;
        IMemoryOwner<int>? repetitionLevels = field.MaxRepetitionLevel > 0 ? MemoryOwner<int>.Allocate((int)numValues) : null;

        try {
            await ReadRawAsync(field, values.Memory, definitionLevels?.Memory, repetitionLevels?.Memory, cancellationToken);
        } catch {
            values.Dispose();
            definitionLevels?.Dispose();
            repetitionLevels?.Dispose();
            throw;
        }

        return new RawColumnData<T>(values, definitionLevels, repetitionLevels);
    }

    internal async ValueTask<object> ReadRawColumnDataAsObjectAsync(DataField field, CancellationToken cancellationToken = default) {
        // use reflection to call generic version of the method
        MethodInfo? method = typeof(ParquetRowGroupReader)
            .GetMethod(nameof(ParquetRowGroupReader.ReadRawColumnDataAsync), BindingFlags.Instance | BindingFlags.NonPublic);
        if(method == null) {
            throw new InvalidOperationException($"can't find {nameof(ParquetRowGroupReader.ReadRawColumnDataAsync)} method on {nameof(ParquetRowGroupReader)}");
        }
        method = method.MakeGenericMethod(field.ClrValueType);

        object? valueTask = method.Invoke(this, [field, cancellationToken]);
        object? result = await (dynamic)valueTask!;
        return result;
    }

    /// <summary>
    /// Reads a column from this row group. Unlike writing, columns can be read in any order. If the column is missing,
    /// an exception will be thrown.
    /// </summary>
    public async ValueTask ReadAsync<T>(DataField field,
        Memory<T> values,
        Memory<int>? repetitionLevels = null,
        CancellationToken cancellationToken = default)
        where T : struct {
        await ReadRawAsync(field, values, null, repetitionLevels, cancellationToken);
    }

    /// <summary>
    /// Read nullable column data
    /// </summary>
    public async ValueTask ReadAsync<T>(DataField field,
        Memory<T?> values,
        Memory<int>? repetitionLevels = null,
        CancellationToken cancellationToken = default)
        where T : struct {

        // allocate memory for non-nullable values and definition levels, which will be used to reconstruct nullable values
        using IMemoryOwner<T> nonNullMemory = MemoryOwner<T>.Allocate(values.Length);
        using IMemoryOwner<int> definitionLevels = MemoryOwner<int>.Allocate((int)RowCount);

        // read non-null version of the column along with definition levels
        await ReadRawAsync(field, nonNullMemory.Memory, definitionLevels.Memory, repetitionLevels, cancellationToken);

        // reconstruct nullable values based on definition levels
        Span<T?> valuesSpan = values.Span;
        Span<T> nonNullSpan = nonNullMemory.Memory.Span;
        Span<int> dlSpan = definitionLevels.Memory.Span;
        int maxDl = field.MaxDefinitionLevel;
        int nni = 0;
        for(int i = 0; i < RowCount; i++) {
            bool isNull = dlSpan[i] < maxDl;
            valuesSpan[i] = isNull ? null : nonNullSpan[nni++];
        }
    }

    /// <summary>
    /// Read column as UTF-8 strings.
    /// </summary>
    public async ValueTask ReadAsync(DataField field,
        Memory<string> values,
        Memory<int>? repetitionLevels = null,
        CancellationToken cancellationToken = default) {

        using IMemoryOwner<int> definitionlevels = MemoryOwner<int>.Allocate(field.MaxDefinitionLevel > 0 ? (int)RowCount : 0);
        using IMemoryOwner<ReadOnlyMemory<char>> rawValues = MemoryOwner<ReadOnlyMemory<char>>.Allocate(values.Length);

        await ReadRawAsync(field, rawValues.Memory, definitionlevels.Memory, repetitionLevels, cancellationToken);

        // convert back to strings
        Span<int> dlSpan = definitionlevels.Memory.Span;
        Span<ReadOnlyMemory<char>> valueSpan = rawValues.Memory.Span;
        for(int i = 0; i < valueSpan.Length; i++) {
            bool isNull = dlSpan[i] == 0;
            values.Span[i] = new string(valueSpan[i].Span);
        }
    }

    /// <summary>
    /// Gets raw column chunk metadata for this field
    /// </summary>
    public ColumnChunk? GetMetadata(DataField field) {
        if(field == null)
            throw new ArgumentNullException(nameof(field));

        if(!_pathToChunk.TryGetValue(field.Path, out ColumnChunk? columnChunk)) {
            return null;
        }

        return columnChunk;
    }

    /// <summary>
    /// Get custom key-value metadata for a data field
    /// </summary>
    public Dictionary<string, string> GetCustomMetadata(DataField field) {
        ColumnChunk? cc = GetMetadata(field);
        if(cc?.MetaData?.KeyValueMetadata == null)
            return new();

        return cc.MetaData.KeyValueMetadata.ToDictionary(kv => kv.Key, kv => kv.Value!);
    }

    private DataColumnStatistics? ReadColumnStatistics(ColumnChunk cc) {

        Statistics? st = cc.MetaData!.Statistics;
        if(st == null)
            return null;

        SchemaElement? se = _footer.GetSchemaElement(cc) ?? throw new ArgumentException("can't find schema element", nameof(cc));

        ParquetPlainEncoder.TryDecode(st.MinValue, se, _options, out object? min);
        ParquetPlainEncoder.TryDecode(st.MaxValue, se, _options, out object? max);

        return new DataColumnStatistics(st.NullCount, st.DistinctCount, min, max);
    }


    /// <summary>
    /// Returns data column statistics for a particular data field
    /// </summary>
    /// <param name="field"></param>
    /// <returns></returns>
    /// <exception cref="ParquetException"></exception>
    public DataColumnStatistics? GetStatistics(DataField field) {
        ColumnChunk cc = GetMetadata(field) ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
        return ReadColumnStatistics(cc);
    }

    /// <summary>
    /// Dispose isn't required, retained for backward compatibility
    /// </summary>
    public void Dispose() { }
}