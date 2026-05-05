using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;
using FieldPath = Parquet.Schema.FieldPath;

namespace Parquet;

/// <summary>
/// Writer for Parquet row groups
/// </summary>
public class ParquetRowGroupWriter : IDisposable {
    private readonly Stream _stream;
    private readonly ThriftFooter _footer;
    private readonly ParquetOptions _options;
    private readonly RowGroup _owGroup;
    private readonly SchemaElement[] _thschema;
    private int _colIdx;

    internal ParquetRowGroupWriter(
       Stream stream,
       ThriftFooter footer,
       ParquetOptions options) {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _footer = footer ?? throw new ArgumentNullException(nameof(footer));
        _options = options;

        _owGroup = _footer.AddRowGroup();
        _owGroup.Columns = new List<ColumnChunk>();
        _thschema = _footer.GetWriteableSchema();
    }

    internal long? RowCount { get; private set; }

    #region [ Helper methods ]

    /// <summary>
    /// Helper method that converts the provided string values to nullable <see cref="ReadOnlyMemory{Char}"/> and calls
    /// the main WriteAsync method. This is useful for writing string columns without having to manually convert them to
    /// the required format.
    /// </summary>
    public async Task WriteAsync(DataField field, IReadOnlyCollection<string?> values,
        ReadOnlyMemory<int>? repetitionLevels = null) {
        ReadOnlyMemory<char>?[] rented = ArrayPool<ReadOnlyMemory<char>?>.Shared.Rent(values.Count);
        try {
            int i = 0;
            foreach(string? s in values)
                rented[i++] = s.AsNullableReadOnlyMemory();
            await WriteAsync(field, new ReadOnlyMemory<ReadOnlyMemory<char>?>(rented, 0, i), repetitionLevels);
        } finally {
            ArrayPool<ReadOnlyMemory<char>?>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Helper method that converts the provided byte array values to nullable <see cref="ReadOnlyMemory{Byte}"/> and
    /// calls the main WriteAsync method. This is useful for writing binary columns without having to manually convert
    /// them to the required format.
    /// </summary>
    /// <param name="field"></param>
    /// <param name="values"></param>
    /// <param name="repetitionLevels"></param>
    /// <returns></returns>
    public async Task WriteAsync(DataField field, IReadOnlyCollection<byte[]?> values,
        ReadOnlyMemory<int>? repetitionLevels = null) {
        ReadOnlyMemory<byte>?[] rented = ArrayPool<ReadOnlyMemory<byte>?>.Shared.Rent(values.Count);
        try {
            int i = 0;
            foreach(byte[]? b in values)
                rented[i++] = b.AsNullableReadOnlyMemory();
            await WriteAsync(field, new ReadOnlyMemory<ReadOnlyMemory<byte>?>(rented, 0, i), repetitionLevels);
        } finally {
            ArrayPool<ReadOnlyMemory<byte>?>.Shared.Return(rented);
        }
    }


    #endregion


    /// <summary>
    /// Writes a column of data to this row group. The column must correspond to the next column in the schema, and all
    /// columns must be written in the order they appear in the schema. The method will validate that the provided field
    /// matches the expected column from the schema, and that all columns have the same row count. If any of these
    /// validations fail, an exception will be thrown.
    /// </summary>
    /// <param name="field">The data field representing the column to write.</param>
    /// <param name="values">The values to write for the column.</param>
    /// <param name="repetitionLevels">Optional repetition levels for the column.</param>
    /// <param name="customMetadata">Optional custom metadata for the column.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    public async Task WriteAsync<T>(DataField field,
        ReadOnlyMemory<T?> values,
        ReadOnlyMemory<int>? repetitionLevels = null,
        Dictionary<string, string>? customMetadata = null,
        CancellationToken cancellationToken = default) where T : struct {

        using WritingColumn<T> wc = WritingColumn<T>.NewWritingColumn(field, values, repetitionLevels);
        await WriteAsyncInternal(field, wc, customMetadata, cancellationToken);
    }


    /// <summary>
    /// Writes a column of data to this row group. The column must correspond to the next column in the schema, and all
    /// columns must be written in the order they appear in the schema. The method will validate that the provided field
    /// matches the expected column from the schema, and that all columns have the same row count. If any of these
    /// validations fail, an exception will be thrown.
    /// </summary>
    /// <param name="field">The data field representing the column to write.</param>
    /// <param name="values">The values to write for the column.</param>
    /// <param name="repetitionLevels">Optional repetition levels for the column.</param>
    /// <param name="customMetadata">Optional custom metadata for the column.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    public async Task WriteAsync<T>(DataField field,
        ReadOnlyMemory<T> values,
        ReadOnlyMemory<int>? repetitionLevels = null,
        Dictionary<string, string>? customMetadata = null,
        CancellationToken cancellationToken = default) where T : struct {

        using WritingColumn<T> wc = WritingColumn<T>.NewWritingColumn(field, values, repetitionLevels);
        await WriteAsyncInternal(field, wc, customMetadata, cancellationToken);
    }

    internal async Task WriteAsyncAllParts<T>(DataField field,
        ReadOnlyMemory<T> values,
        ReadOnlyMemory<int>? definitionValues,
        ReadOnlyMemory<int>? repetitionLevels,
        CancellationToken cancellationToken) where T : struct {
        using WritingColumn<T> wc = WritingColumn<T>.NewWritingColumn(field, values, definitionValues, repetitionLevels);
        await WriteAsyncInternal(field, wc, null, cancellationToken);
    }

    internal async Task WriteAsync<T>(DataField field, RawColumnData<T> data, CancellationToken cancellationToken) where T : struct {
        await WriteAsyncAllParts(field, data.ValuesMemory, data.DefinitionLevelsMemoryOrNull, data.RepetitionLevelsMemoryOrNull, cancellationToken);
    }

    internal async Task WriteAsync(DataField field, RawColumnData data, CancellationToken cancellationToken) {
        ArgumentNullException.ThrowIfNull(field);
        ArgumentNullException.ThrowIfNull(data);

        System.Type dataType = data.GetType();
        if(!dataType.IsGenericType || dataType.GetGenericTypeDefinition() != typeof(RawColumnData<>))
            throw new ArgumentException("expected RawColumnData<T> instance", nameof(data));

        System.Type elementType = dataType.GetGenericArguments()[0];

        MethodInfo? writeAsyncGenericDefinition = typeof(ParquetRowGroupWriter)
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
            .SingleOrDefault(m =>
                m.Name == nameof(WriteAsync) &&
                m.IsGenericMethodDefinition &&
                m.GetGenericArguments().Length == 1 &&
                m.GetParameters() is { Length: 3 } p &&
                p[0].ParameterType == typeof(DataField) &&
                p[1].ParameterType.IsGenericType &&
                p[1].ParameterType.GetGenericTypeDefinition() == typeof(RawColumnData<>) &&
                p[2].ParameterType == typeof(CancellationToken));

        if(writeAsyncGenericDefinition == null)
            throw new InvalidOperationException("failed to locate generic WriteAsync overload");

        MethodInfo closedWriteAsync = writeAsyncGenericDefinition.MakeGenericMethod(elementType);
        Task writeTask = (Task?)closedWriteAsync.Invoke(this, new object[] { field, data, cancellationToken })
            ?? throw new InvalidOperationException("generic WriteAsync invocation returned null task");

        await writeTask;
    }

    private async Task WriteAsyncInternal<T>(DataField field,
        WritingColumn<T> wc,
        Dictionary<string, string>? customMetadata,
        CancellationToken cancellationToken) where T : struct {

        if(field == null)
            throw new ArgumentNullException(nameof(field));

        if(_colIdx >= _thschema.Length) {
            throw new InvalidOperationException(
                $"Cannot write column '{field.Name}': all {_thschema.Length} columns from the schema have already been written. " +
                "You may have called WriteColumnAsync more times than there are columns in the schema.");
        }

        SchemaElement tse = _thschema[_colIdx];
        if(!field.Equals(tse)) {
            throw new ArgumentException($"cannot write this column, expected '{tse.Name}', passed: '{field.Name}'", nameof(field));
        }
        _colIdx += 1;

        FieldPath path = _footer.GetPath(tse);

        var writer = new DataColumnWriter(_stream, _footer, tse, _options, customMetadata);

        if(RowCount == null) {
            RowCount = wc.CalculateRowCount();
        } else {
            // Validate that all columns have the same row count
            long columnRowCount = wc.CalculateRowCount();
            if(columnRowCount != RowCount) {
                throw new InvalidOperationException(
                    $"Column '{field.Name}' has {columnRowCount} rows, but the row group expects {RowCount} rows. " +
                    "All columns in a row group must have the same number of rows.");
            }
        }

        ColumnChunk chunk = await writer.WriteAsync(path, wc, cancellationToken);
        _owGroup.Columns.Add(chunk);
    }

    /// <summary>
    /// Call to indicate that all columns have been written, to validate completeness.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when not all columns from the schema have been written.
    /// </exception>
    public void CompleteValidate() {
        // This code used to live in Dispose, but Dispose must not throw exceptions, see issue 666.

        // Check if all columns are present
        if(_colIdx < _thschema.Length) {
            throw new InvalidOperationException(
                $"Not all columns were written. Expected {_thschema.Length} columns but only {_colIdx} were written. " +
                $"Missing columns: {string.Join(", ", _thschema.Skip(_colIdx).Select(s => s.Name))}");
        }
    }

    /// <summary>
    /// Finalizes the row group writing by updating row count and size metadata.
    /// </summary>
    public void Dispose() {
        //row count is known only after at least one column is written
        _owGroup.NumRows = RowCount ?? 0;

        //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
        //luckily ColumnChunk already contains sizes of page+header in it's meta
        _owGroup.TotalCompressedSize = _owGroup.Columns.Sum(c => c.MetaData!.TotalCompressedSize);
        _owGroup.TotalByteSize = _owGroup.Columns.Sum(c => c.MetaData!.TotalUncompressedSize);
    }
}