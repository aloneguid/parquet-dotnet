using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance.Buffers;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;

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
    public long RowCount => _rowGroup.NumRows;

    /// <summary>
    /// Checks if this field exists in source schema
    /// </summary>
    public bool ColumnExists(DataField field) {
        return GetMetadata(field) != null;
    }

    /// <summary>
    /// Reads a column from this row group. Unlike writing, columns can be read in any order.
    /// If the column is missing, an exception will be thrown.
    /// </summary>
    public Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default) {
        ColumnChunk columnChunk = GetMetadata(field)
            ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
        var columnReader = new DataColumnReader(field, _stream,
            columnChunk, ReadColumnStatistics(columnChunk), _footer, _options);
        return columnReader.ReadAsync(cancellationToken);
    }

    /// <summary>
    /// Performs "raw" reading of a column, which returns non-nullable values and definition/repetition levels as separate buffers.
    /// Intended for advanced use only.
    /// todo: explain what to use instead
    /// </summary>
    public async ValueTask ReadAsyncRaw<T>(DataField field,
        Memory<T> values,
        Memory<int>? definitionLevels,
        Memory<int>? repetitionLevels,
        CancellationToken cancellationToken = default)
        where T : struct {

        if(field.MaxDefinitionLevel > 0 && definitionLevels == null)
            throw new ArgumentException($"Definition levels buffer is required for field '{field.Path}' ({nameof(Field.MaxDefinitionLevel)} = {field.MaxDefinitionLevel})");

        if(field.MaxRepetitionLevel > 0 && repetitionLevels == null)
            throw new ArgumentException($"Repetition levels buffer is required for field '{field.Path}' ({nameof(Field.MaxRepetitionLevel)} = {field.MaxRepetitionLevel})");

        if(definitionLevels != null && definitionLevels.Value.Length < RowCount)
            throw new ArgumentException($"Definition levels buffer is too small for field '{field.Path}' (length {definitionLevels.Value.Length} < row count {RowCount})");

        if(repetitionLevels != null && repetitionLevels.Value.Length < RowCount)
            throw new ArgumentException($"Repetition levels buffer is too small for field '{field.Path}' (length {repetitionLevels.Value.Length} < row count {RowCount})");

        if(field.IsNullable) {
            // todo: validate that buffer is big enough to hold all values (it doesn't have to be as big as row count, but it has to be big enough to hold all non-null values)
        } else {
            if(values.Length < RowCount)
                throw new ArgumentException($"Values buffer is too small for field '{field.Path}' (length {values.Length} < row count {RowCount})");
        }

        ColumnChunk columnChunk = GetMetadata(field)
            ?? throw new ParquetException($"'{field.Path}' does not exist in this file");

        var columnReader = new DataColumnReader(field, _stream,
            columnChunk, ReadColumnStatistics(columnChunk), _footer, _options);

        // todo: validate there's enough memory to read back

        using var rc = new ReadingColumn<T>(field, values, definitionLevels, repetitionLevels);
        await columnReader.ReadAsync(rc, cancellationToken);
    }

    /// <summary>
    /// Read non-nullable column data
    /// </summary>
    public async ValueTask ReadAsync<T>(DataField field,
        Memory<T> values,
        Memory<int>? repetitionLevels = null,
        CancellationToken cancellationToken = default)
        where T : struct {
        await ReadAsyncRaw(field, values, null, repetitionLevels, cancellationToken);
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
        await ReadAsyncRaw(field, nonNullMemory.Memory, definitionLevels.Memory, repetitionLevels, cancellationToken);

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

        await ReadAsyncRaw(field, rawValues.Memory, definitionlevels.Memory, repetitionLevels, cancellationToken);

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