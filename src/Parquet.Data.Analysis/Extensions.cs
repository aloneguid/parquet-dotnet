using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Analysis;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Defines extension methods to simplify Parquet usage
/// </summary>
public static class AnalysisExtensions {

    /// <summary>
    /// Reads a Parquet file from a stream and returns a DataFrame.
    /// </summary>
    public static async Task<DataFrame> ReadParquetAsDataFrameAsync(
        this Stream inputStream, CancellationToken cancellationToken = default) {

        await using ParquetReader reader = await ParquetReader.CreateAsync(inputStream, cancellationToken: cancellationToken);

        List<DataField> fields = reader.Schema.Fields.OfType<DataField>().ToList();
        var columns = new DataFrameColumn[fields.Count];

        for(int rgIndex = 0; rgIndex < reader.RowGroupCount; rgIndex++) {
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(rgIndex);
            int rowCount = (int)rgr.RowCount;

            for(int i = 0; i < fields.Count; i++) {
                DataField field = fields[i];
                string colName = string.Join("_", field.Path.ToList());

                if(rgIndex == 0) {
                    columns[i] = await CreateColumnAsync(rgr, field, rowCount, colName, cancellationToken);
                } else {
                    await AppendToColumnAsync(rgr, field, rowCount, columns[i], cancellationToken);
                }
            }
        }

        return new DataFrame(columns);
    }

    /// <summary>
    /// Writes a DataFrame to a Parquet file.
    /// </summary>
    public static async Task WriteAsync(
        this DataFrame df, Stream outputStream, CancellationToken cancellationToken = default) {

        var schema = new ParquetSchema(
            df.Columns.Select(col => new DataField(col.Name, col.DataType.GetNullable())));

        await using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, outputStream, cancellationToken: cancellationToken);
        using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

        for(int i = 0; i < df.Columns.Count; i++) {
            DataFrameColumn col = df.Columns[i] ?? throw new InvalidOperationException("unexpected null column");
            await WriteColumnAsync(schema.DataFields[i], col, rgw);
        }
    }

    private static async Task<DataFrameColumn> CreateColumnAsync(
        ParquetRowGroupReader rgr, DataField field, int rowCount, string name, CancellationToken ct) {

        Type t = field.ClrType;

        if(t == typeof(bool)) return new BooleanDataFrameColumn(name, await ReadAsync<bool>(rgr, field, rowCount, ct));
        if(t == typeof(int)) return new Int32DataFrameColumn(name, await ReadAsync<int>(rgr, field, rowCount, ct));
        if(t == typeof(uint)) return new UInt32DataFrameColumn(name, await ReadAsync<uint>(rgr, field, rowCount, ct));
        if(t == typeof(long)) return new Int64DataFrameColumn(name, await ReadAsync<long>(rgr, field, rowCount, ct));
        if(t == typeof(ulong)) return new UInt64DataFrameColumn(name, await ReadAsync<ulong>(rgr, field, rowCount, ct));
        if(t == typeof(byte)) return new ByteDataFrameColumn(name, await ReadAsync<byte>(rgr, field, rowCount, ct));
        if(t == typeof(sbyte)) return new SByteDataFrameColumn(name, await ReadAsync<sbyte>(rgr, field, rowCount, ct));
        if(t == typeof(short)) return new Int16DataFrameColumn(name, await ReadAsync<short>(rgr, field, rowCount, ct));
        if(t == typeof(ushort)) return new UInt16DataFrameColumn(name, await ReadAsync<ushort>(rgr, field, rowCount, ct));
        if(t == typeof(float)) return new SingleDataFrameColumn(name, await ReadAsync<float>(rgr, field, rowCount, ct));
        if(t == typeof(double)) return new DoubleDataFrameColumn(name, await ReadAsync<double>(rgr, field, rowCount, ct));
        if(t == typeof(decimal)) return new DecimalDataFrameColumn(name, await ReadAsync<decimal>(rgr, field, rowCount, ct));
        if(t == typeof(DateTime)) return new DateTimeDataFrameColumn(name, await ReadAsync<DateTime>(rgr, field, rowCount, ct));
        if(t == typeof(TimeSpan)) return new PrimitiveDataFrameColumn<TimeSpan>(name, await ReadAsync<TimeSpan>(rgr, field, rowCount, ct));
        if(t == typeof(string)) return new StringDataFrameColumn(name, await ReadStringsAsync(rgr, field, rowCount, ct));

        throw new NotSupportedException($"unsupported column type {t}");
    }

    private static async Task AppendToColumnAsync(
        ParquetRowGroupReader rgr, DataField field, int rowCount, DataFrameColumn col, CancellationToken ct) {

        Type t = field.ClrType;

        if(t == typeof(bool)) { Append((PrimitiveDataFrameColumn<bool>)col, await ReadAsync<bool>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(int)) { Append((PrimitiveDataFrameColumn<int>)col, await ReadAsync<int>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(uint)) { Append((PrimitiveDataFrameColumn<uint>)col, await ReadAsync<uint>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(long)) { Append((PrimitiveDataFrameColumn<long>)col, await ReadAsync<long>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(ulong)) { Append((PrimitiveDataFrameColumn<ulong>)col, await ReadAsync<ulong>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(byte)) { Append((PrimitiveDataFrameColumn<byte>)col, await ReadAsync<byte>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(sbyte)) { Append((PrimitiveDataFrameColumn<sbyte>)col, await ReadAsync<sbyte>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(short)) { Append((PrimitiveDataFrameColumn<short>)col, await ReadAsync<short>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(ushort)) { Append((PrimitiveDataFrameColumn<ushort>)col, await ReadAsync<ushort>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(float)) { Append((PrimitiveDataFrameColumn<float>)col, await ReadAsync<float>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(double)) { Append((PrimitiveDataFrameColumn<double>)col, await ReadAsync<double>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(decimal)) { Append((PrimitiveDataFrameColumn<decimal>)col, await ReadAsync<decimal>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(DateTime)) { Append((PrimitiveDataFrameColumn<DateTime>)col, await ReadAsync<DateTime>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(TimeSpan)) { Append((PrimitiveDataFrameColumn<TimeSpan>)col, await ReadAsync<TimeSpan>(rgr, field, rowCount, ct)); return; }
        if(t == typeof(string)) { Append((StringDataFrameColumn)col, await ReadStringsAsync(rgr, field, rowCount, ct)); return; }

        throw new NotSupportedException($"unsupported column type {t}");
    }

    private static async Task<T?[]> ReadAsync<T>(
        ParquetRowGroupReader rgr, DataField field, int rowCount, CancellationToken ct) where T : struct {

        T?[] data = new T?[rowCount];
        await rgr.ReadAsync(field, data.AsMemory(), null, ct);
        return data;
    }

    private static async Task<string?[]> ReadStringsAsync(
        ParquetRowGroupReader rgr, DataField field, int rowCount, CancellationToken ct) {

        string?[] data = new string[rowCount];
        await rgr.ReadAsync(field, data.AsMemory()!, null, ct);
        return data;
    }

    private static void Append<T>(PrimitiveDataFrameColumn<T> col, T?[] data) where T : unmanaged {
        foreach(T? v in data) {
            col.Append(v);
        }
    }

    private static void Append(StringDataFrameColumn col, string?[] data) {
        foreach(string? v in data) {
            col.Append(v);
        }
    }

    private static async Task WriteColumnAsync(DataField field, DataFrameColumn col, ParquetRowGroupWriter rgw) {
        Type t = col.DataType;

        if(t == typeof(bool)) { await rgw.WriteAsync<bool>(field, ((PrimitiveDataFrameColumn<bool>)col).ToArray().AsMemory()); return; }
        if(t == typeof(int)) { await rgw.WriteAsync<int>(field, ((PrimitiveDataFrameColumn<int>)col).ToArray().AsMemory()); return; }
        if(t == typeof(uint)) { await rgw.WriteAsync<uint>(field, ((PrimitiveDataFrameColumn<uint>)col).ToArray().AsMemory()); return; }
        if(t == typeof(long)) { await rgw.WriteAsync<long>(field, ((PrimitiveDataFrameColumn<long>)col).ToArray().AsMemory()); return; }
        if(t == typeof(ulong)) { await rgw.WriteAsync<ulong>(field, ((PrimitiveDataFrameColumn<ulong>)col).ToArray().AsMemory()); return; }
        if(t == typeof(byte)) { await rgw.WriteAsync<byte>(field, ((PrimitiveDataFrameColumn<byte>)col).ToArray().AsMemory()); return; }
        if(t == typeof(sbyte)) { await rgw.WriteAsync<sbyte>(field, ((PrimitiveDataFrameColumn<sbyte>)col).ToArray().AsMemory()); return; }
        if(t == typeof(short)) { await rgw.WriteAsync<short>(field, ((PrimitiveDataFrameColumn<short>)col).ToArray().AsMemory()); return; }
        if(t == typeof(ushort)) { await rgw.WriteAsync<ushort>(field, ((PrimitiveDataFrameColumn<ushort>)col).ToArray().AsMemory()); return; }
        if(t == typeof(float)) { await rgw.WriteAsync<float>(field, ((PrimitiveDataFrameColumn<float>)col).ToArray().AsMemory()); return; }
        if(t == typeof(double)) { await rgw.WriteAsync<double>(field, ((PrimitiveDataFrameColumn<double>)col).ToArray().AsMemory()); return; }
        if(t == typeof(decimal)) { await rgw.WriteAsync<decimal>(field, ((PrimitiveDataFrameColumn<decimal>)col).ToArray().AsMemory()); return; }
        if(t == typeof(DateTime)) { await rgw.WriteAsync<DateTime>(field, ((PrimitiveDataFrameColumn<DateTime>)col).ToArray().AsMemory()); return; }
        if(t == typeof(TimeSpan)) { await rgw.WriteAsync<TimeSpan>(field, ((PrimitiveDataFrameColumn<TimeSpan>)col).ToArray().AsMemory()); return; }
        if(t == typeof(string)) {
            string[] strings = ((StringDataFrameColumn)col).ToArray();
            ReadOnlyMemory<char>?[] roms = strings
                .Select(x => x == null ? null : (ReadOnlyMemory<char>?)x.AsMemory())
                .ToArray();
            await rgw.WriteAsync<ReadOnlyMemory<char>>(field, roms);
            return;
        }

        throw new NotSupportedException($"unsupported column type {t}");
    }
}
