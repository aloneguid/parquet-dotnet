using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;
using SType = System.Type;
using FieldPath = Parquet.Schema.FieldPath;

namespace Parquet; 
/// <summary>
/// Writer for Parquet row groups
/// </summary>
#pragma warning disable CA1063 // Implement IDisposable Correctly
public class ParquetRowGroupWriter : IDisposable
#pragma warning restore CA1063 // Implement IDisposable Correctly
{
    private static readonly MethodInfo WriteStructArrayAsyncMethod = typeof(ParquetRowGroupWriter)
        .GetMethod(nameof(WriteStructArrayAsync), BindingFlags.Instance | BindingFlags.NonPublic)!;

    private readonly ParquetSchema _schema;
    private readonly Stream _stream;
    private readonly ThriftFooter _footer;
    private readonly CompressionMethod _compressionMethod;
    private readonly CompressionLevel _compressionLevel;
    private readonly ParquetOptions _formatOptions;
    private readonly RowGroup _owGroup;
    private readonly SchemaElement[] _thschema;
    private int _colIdx;

    internal ParquetRowGroupWriter(ParquetSchema schema,
       Stream stream,
       ThriftFooter footer,
       CompressionMethod compressionMethod,
       ParquetOptions formatOptions,
       CompressionLevel compressionLevel) {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _footer = footer ?? throw new ArgumentNullException(nameof(footer));
        _compressionMethod = compressionMethod;
        _compressionLevel = compressionLevel;
        _formatOptions = formatOptions;

        _owGroup = _footer.AddRowGroup();
        _owGroup.Columns = new List<ColumnChunk>();
        _thschema = _footer.GetWriteableSchema();
    }

    internal long? RowCount { get; private set; }

    /// <summary>
    /// Writes next data column to parquet stream. Note that columns must be written in the order they are declared in the
    /// file schema.
    /// </summary>
    /// <param name="column"></param>
    /// <param name="customMetadata">If specified, adds custom column chunk metadata</param>
    /// <param name="cancellationToken"></param>
    private async Task WriteColumnAsync_TOMIGRATE(DataColumn column,
        Dictionary<string, string>? customMetadata,
        CancellationToken cancellationToken = default) {
        if(column == null)
            throw new ArgumentNullException(nameof(column));

        if(RowCount == null) {
            if(column.NumValues > 0 || column.Field.MaxRepetitionLevel == 0)
                RowCount = column.CalculateRowCount();
        } else {
            // Validate that all columns have the same row count
            long columnRowCount = column.CalculateRowCount();
            if(columnRowCount != RowCount) {
                throw new InvalidOperationException(
                    $"Column '{column.Field.Name}' has {columnRowCount} rows, but the row group expects {RowCount} rows. " +
                    "All columns in a row group must have the same number of rows.");
            }
        }

        if(_colIdx >= _thschema.Length) {
            throw new InvalidOperationException(
                $"Cannot write column '{column.Field.Name}': all {_thschema.Length} columns from the schema have already been written. " +
                "You may have called WriteColumnAsync more times than there are columns in the schema.");
        }

        SchemaElement tse = _thschema[_colIdx];
        if(!column.Field.Equals(tse)) {
            throw new ArgumentException($"cannot write this column, expected '{tse.Name}', passed: '{column.Field.Name}'", nameof(column));
        }
        _colIdx += 1;

        FieldPath path = _footer.GetPath(tse);

        var writer = new DataColumnWriter(_stream, _footer, tse,
           _compressionMethod,
           _formatOptions,
           _compressionLevel,
           customMetadata);

        ColumnChunk chunk = await writer.WriteAsync(path, column, cancellationToken);
        _owGroup.Columns.Add(chunk);

    }

    private Task WriteStructArrayAsync<T>(DataField field, Array values, ReadOnlyMemory<int>? repetitionLevels) where T : struct {
        if(field.IsNullable) {
            if(values is T?[] nullableValues) {
                return WriteAsync<T>(field, nullableValues.AsMemory(), repetitionLevels);
            }

            if(values is T[] nonNullableValues) {
                var promotedValues = new T?[nonNullableValues.Length];
                for(int i = 0; i < nonNullableValues.Length; i++) {
                    promotedValues[i] = nonNullableValues[i];
                }

                return WriteAsync<T>(field, promotedValues.AsMemory(), repetitionLevels);
            }
        } else {
            if(values is T[] nonNullableValues) {
                return WriteAsync<T>(field, nonNullableValues.AsMemory(), repetitionLevels);
            }

            if(values is T?[] nullableValues) {
                var materializedValues = new T[nullableValues.Length];
                for(int i = 0; i < nullableValues.Length; i++) {
                    T? value = nullableValues[i];
                    if(!value.HasValue)
                        throw new ArgumentException($"column '{field.Name}' is not nullable but values contain nulls", nameof(values));

                    materializedValues[i] = value.Value;
                }

                return WriteAsync<T>(field, materializedValues.AsMemory(), repetitionLevels);
            }
        }

        throw new ArgumentException($"expected values of type {GetExpectedArrayType(field)} but passed {values.GetType()}", nameof(values));
    }

    private static SType GetExpectedArrayType(DataField field) {
        SType elementType = field.IsNullable && field.ClrType.IsValueType
            ? typeof(Nullable<>).MakeGenericType(field.ClrType)
            : field.ClrType;

        return elementType.MakeArrayType();
    }

    #region [ Helper methods ]

    /// <summary>
    /// todo
    /// </summary>
    public async Task WriteAsync(DataField field, IEnumerable<string?> values,
        ReadOnlyMemory<int>? repetitionLevels = null) {
        ReadOnlyMemory<ReadOnlyMemory<char>?> memValues = values.Select(s => s.AsNullableReadOnlyMemory()).ToArray();
        await WriteAsync(field, memValues, repetitionLevels);
    }

    /// <summary>
    /// todo
    /// </summary>
    public async Task WriteAsync(DataField field, IEnumerable<byte[]?> values,
        ReadOnlyMemory<int>? repetitionLevels = null) {
        ReadOnlyMemory<ReadOnlyMemory<byte>?> memValues = values.Select(b => b.AsNullableReadOnlyMemory()).ToArray();
        await WriteAsync(field, memValues, repetitionLevels);
    }


    #endregion

    /// <summary>
    /// XP
    /// </summary>
    public async Task WriteAsync<T>(DataField field,
        ReadOnlyMemory<T?> values,
        ReadOnlyMemory<int>? repetitionLevels = null,
        Dictionary<string, string>? customMetadata = null,
        CancellationToken cancellationToken = default) where T : struct {
        
        using WritingColumn<T> wc = WritingColumn<T>.NewWritingColumn(field, values, repetitionLevels);
        await WriteAsyncInternal(field, wc, customMetadata, cancellationToken);
    }

    /// <summary>
    /// XP
    /// </summary>
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


    /// <summary>
    /// XP
    /// </summary>
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

        var writer = new DataColumnWriter(_stream, _footer, tse,
           _compressionMethod,
           _formatOptions,
           _compressionLevel,
           customMetadata);

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
    /// <exception cref="InvalidOperationException">Thrown when not all columns from the schema have been written.</exception>
    public void CompleteValidate() {
        // note: this code used to live in Dispose, but Dispose must not throw exceptions, see issue 666.

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