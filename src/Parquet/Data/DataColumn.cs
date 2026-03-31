using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.Data;

/// <summary>
/// The primary low-level structure to hold data for a parquet column.
/// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
/// </summary>
public class DataColumn {

    private Array? _data;

    private DataColumn(DataField field, Array data, int[]? definitionLevels, int[]? repetitionLevels, bool dataMayContainNulls) {
        if(field is null)
            throw new ArgumentNullException(nameof(field));
        field.EnsureAttachedToSchema(nameof(field));
        if(data is null)
            throw new ArgumentNullException(nameof(data));

        // validate repetition levels
        if(field.MaxRepetitionLevel == 0 && repetitionLevels != null)
            throw new ArgumentException($"this column must not have any repetition levels", nameof(repetitionLevels));
        else if(field.MaxRepetitionLevel > 0 && repetitionLevels == null && data.Length > 0)
            throw new ArgumentNullException(nameof(repetitionLevels), $"repetition levels are required (RL={field.MaxRepetitionLevel})");

        // validate definiton levels
        if(field.MaxDefinitionLevel == 0 && definitionLevels != null) {
            throw new ArgumentException($"this column must not have any definition levels", nameof(definitionLevels));
        } else {

            // validate data type of data
            Type expectedType = dataMayContainNulls ? field.ClrNullableIfHasNullsType : field.ClrType;
            Type actualType = data.GetType().GetElementType()!;
            if(actualType != expectedType)
                throw new ArgumentException($"expected {expectedType}[] but passed {actualType}[]", nameof(data));

            if(dataMayContainNulls && field.MaxDefinitionLevel > 0) {
                int nullCount = data.CalculateNullCountFast(0, data.Length);
                _data = data;
                DefinedData = field.CreateArray(data.Length - nullCount);
                DefinitionLevels = new int[data.Length];
                data.PackNullsFast(0, data.Length, DefinedData, DefinitionLevels, field.MaxDefinitionLevel);
                Statistics.NullCount = nullCount;
            } else {

                if(field.MaxDefinitionLevel > 0 && definitionLevels == null && data.Length > 0) {
                    throw new ArgumentNullException(nameof(definitionLevels), $"definition levels are required (DL={field.MaxDefinitionLevel})");
                }

                DefinedData = data;
                DefinitionLevels = definitionLevels;
                Statistics.NullCount = definitionLevels == null
                    ? 0
                    : definitionLevels.Count(l => l != field.MaxDefinitionLevel);
            }
        }

        Field = field ?? throw new ArgumentNullException(nameof(field));
        RepetitionLevels = repetitionLevels;
    }

    /// <summary>
    /// Creates a new instance of DataColumn from essential parts of Parquet data.
    /// </summary>
    /// <param name="field">Data field associated with this data column.</param>
    /// <param name="definedData">Raw data as it, without definition levels applied</param>
    /// <param name="repetitionLevels">Repetition levels, if required.</param>
    /// <param name="definitionLevels">Definition levels, if required.</param>
    /// <exception cref="ArgumentNullException"></exception>
    public DataColumn(DataField field, Array definedData, int[]? definitionLevels, int[]? repetitionLevels)
        : this(field, definedData, definitionLevels, repetitionLevels, false) {
    }


    /// <summary>
    /// 
    /// </summary>
    /// <param name="field"></param>
    /// <param name="data"></param>
    /// <param name="repetitionLevels"></param>
    public DataColumn(DataField field, Array data, int[]? repetitionLevels = null)
        : this(field, data, null, repetitionLevels, true) {
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="field"></param>
    /// <param name="data"></param>
    public DataColumn(DataField field, Array data) : this(field, data, null, null, true) {

    }

    /// <summary>
    /// Data field
    /// </summary>
    public DataField Field { get; private set; }

    /// <summary>
    /// Defined data. If definition levels are present, they are not applied here.
    /// </summary>
    public Array DefinedData { get; private set; }

    /// <summary>
    /// Column data where definition levels are already applied
    /// </summary>
    public Array Data {
        get {
            if(!HasDefinitions)
                return DefinedData;

            if(_data == null) {
                _data = Field.UnpackDefinitions(DefinedData, DefinitionLevels);
            }

            return _data;
        }
    }

    /// <summary>
    /// Definition levels, if present
    /// </summary>
    public int[]? DefinitionLevels { get; }

    /// <summary>
    /// Repetition levels if any.
    /// </summary>
    public int[]? RepetitionLevels { get; }

    /// <summary>
    /// When true, this field has repetitions. It doesn't mean that it's an array though. This property simply checks that
    /// repetition levels are present on this column.
    /// </summary>
    public bool HasRepetitions => RepetitionLevels != null;

    /// <summary>
    /// When true, this column has definition levels
    /// </summary>
    public bool HasDefinitions => DefinitionLevels != null;

    /// <summary>
    /// Basic statistics for this data column (populated only on read)
    /// </summary>
    public DataColumnStatistics Statistics { get; internal set; } =
        new DataColumnStatistics(null, null, null, null);

    /// <summary>
    /// Number of actual values in this column, including nulls and null/empty lists polyfills.
    /// </summary>
    public int NumValues => DefinitionLevels?.Length ?? DefinedData.Length;

    /// <summary>
    /// Casts <see cref="Data"/> to <see cref="Span{T}"/>
    /// </summary>
    /// <exception cref="InvalidOperationException">When T is invalid type</exception>
    public Span<T> AsSpan<T>(int? offset = null, int? count = null) {
        if(Data is not T[] ar)
            throw new InvalidOperationException($"data is not castable to {typeof(T)}[]");

        Span<T> span = ar.AsSpan();
        if(offset != null)
            span = span.Slice(offset.Value);
        if(count != null)
            span = span.Slice(0, count.Value);
        return span;
    }

    /// <summary>
    /// Concatenates multiple columns into a single column. All columns must have the same schema.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public static DataColumn Concat(IEnumerable<DataColumn> columns) {
        // validate all columns have the same schema
        DataField firstField = columns.First().Field;
        if(columns.Any(c => !c.Field.Equals(firstField)))
            throw new ArgumentException("all columns must have the same schema");

        // there are 3 things we need to copy: definedData, definitionLevels, repetitionLevels
        int definedDataLength = columns.Sum(c => c.DefinedData.Length);
        int definitionLevelsLength = columns.Sum(c => c.DefinitionLevels?.Length ?? 0);
        int repetitionLevelsLength = columns.Sum(c => c.RepetitionLevels?.Length ?? 0);

        // concatenate defined data
        Array definedData = firstField.CreateArray(definedDataLength);
        int offset = 0;
        foreach(DataColumn column in columns) {
            column.DefinedData.CopyTo(definedData, offset);
            offset += column.DefinedData.Length;
        }

        // concatenate definition levels
        int[]? definitionLevels = null;
        if(definitionLevelsLength > 0) {
            definitionLevels = new int[definitionLevelsLength];
            offset = 0;
            foreach(DataColumn column in columns) {
                if(column.DefinitionLevels != null) {
                    column.DefinitionLevels.CopyTo(definitionLevels, offset);
                    offset += column.DefinitionLevels.Length;
                }
            }
        }

        // concatenate repetition levels
        int[]? repetitionLevels = null;
        if(repetitionLevelsLength > 0) {
            repetitionLevels = new int[repetitionLevelsLength];
            offset = 0;
            foreach(DataColumn column in columns) {
                if(column.RepetitionLevels != null) {
                    column.RepetitionLevels.CopyTo(repetitionLevels, offset);
                    offset += column.RepetitionLevels.Length;
                }
            }
        }

        // build the resulting column
        return new DataColumn(firstField, definedData, definitionLevels, repetitionLevels);
    }

    internal bool IsDeltaEncodable => DefinedData.Length > 0 && Field.IsDeltaEncodable;

    internal long CalculateRowCount() =>
        Field.MaxRepetitionLevel > 0
            ? RepetitionLevels?.Count(rl => rl == 0) ?? 0
            : NumValues;

    /// <inheritdoc/>
    public override string ToString() {
        return Field.ToString();
    }
}