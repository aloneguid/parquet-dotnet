using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Analysis;
using Parquet.Data;
using Parquet.Data.Analysis;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Defines extension methods to simplify Parquet usage
/// </summary>
public static class AnalysisExtensions {
    /// <summary>
    /// 
    /// </summary>
    /// <param name="inputStream"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<DataFrame> ReadParquetAsDataFrameAsync(
        this Stream inputStream, CancellationToken cancellationToken = default) {
        using ParquetReader reader = await ParquetReader.CreateAsync(inputStream, cancellationToken: cancellationToken);

        var dfcs = new List<DataFrameColumn>();
        //var readableFields = reader.Schema.DataFields.Where(df => df.MaxRepetitionLevel == 0).ToList();
        List<DataField> readableFields = reader.Schema.Fields
            .Select(df => df as DataField)
            .Where(df => df != null)
            .Cast<DataField>()
            .ToList();
        var columns = new List<DataFrameColumn>();

        for(int rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++) {
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(rowGroupIndex);

            for(int dataFieldIndex = 0; dataFieldIndex < readableFields.Count; dataFieldIndex++) {
                DataColumn dc = await rgr.ReadColumnAsync(readableFields[dataFieldIndex], cancellationToken);

                if(rowGroupIndex == 0) {
                    dfcs.Add(DataFrameMapper.ToDataFrameColumn(dc));
                } else {
                    DataFrameMapper.AppendValues(dfcs[dataFieldIndex], dc);
                }
            }
        }

        return new DataFrame(dfcs);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="df"></param>
    /// <param name="outputStream"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task WriteAsync(this DataFrame df, Stream outputStream, CancellationToken cancellationToken = default) {
        // create schema
        var schema = new ParquetSchema(
            df.Columns.Select(col => new DataField(col.Name, col.DataType.GetNullable())));

        await using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, outputStream, cancellationToken: cancellationToken);
        using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

        int i = 0;
        foreach(DataFrameColumn? col in df.Columns) {
            if(col == null)
                throw new InvalidOperationException("unexpected null column");

            DataField field = schema.DataFields[i];

            await WriteAsync(field, col, rgw);

            i += 1;
        }
    }

    private static async Task WriteAsync(DataField field, DataFrameColumn col, ParquetRowGroupWriter rgw) {
        if(col.DataType == typeof(bool)) {
            Memory<bool?> mo = ((PrimitiveDataFrameColumn<bool>)col).ToArray().AsMemory();
            await rgw.WriteAsync<bool>(field, mo);
        } else if(col.DataType == typeof(int)) {
            Memory<int?> mo = ((PrimitiveDataFrameColumn<int>)col).ToArray().AsMemory();
            await rgw.WriteAsync<int>(field, mo);
        } else if(col.DataType == typeof(uint)) {
            Memory<uint?> mo = ((PrimitiveDataFrameColumn<uint>)col).ToArray().AsMemory();
            await rgw.WriteAsync<uint>(field, mo);
        } else if(col.DataType == typeof(long)) {
            Memory<long?> mo = ((PrimitiveDataFrameColumn<long>)col).ToArray().AsMemory();
            await rgw.WriteAsync<long>(field, mo);
        } else if(col.DataType == typeof(ulong)) {
            Memory<ulong?> mo = ((PrimitiveDataFrameColumn<ulong>)col).ToArray().AsMemory();
            await rgw.WriteAsync<ulong>(field, mo);
        } else if(col.DataType == typeof(byte)) {
            Memory<byte?> mo = ((PrimitiveDataFrameColumn<byte>)col).ToArray().AsMemory();
            await rgw.WriteAsync<byte>(field, mo);
        } else if(col.DataType == typeof(sbyte)) {
            Memory<sbyte?> mo = ((PrimitiveDataFrameColumn<sbyte>)col).ToArray().AsMemory();
            await rgw.WriteAsync<sbyte>(field, mo);
        } else if(col.DataType == typeof(short)) {
            Memory<short?> mo = ((PrimitiveDataFrameColumn<short>)col).ToArray().AsMemory();
            await rgw.WriteAsync<short>(field, mo);
        } else if(col.DataType == typeof(ushort)) {
            Memory<ushort?> mo = ((PrimitiveDataFrameColumn<ushort>)col).ToArray().AsMemory();
            await rgw.WriteAsync<ushort>(field, mo);
        } else if(col.DataType == typeof(DateTime)) {
            Memory<DateTime?> mo = ((PrimitiveDataFrameColumn<DateTime>)col).ToArray().AsMemory();
            await rgw.WriteAsync<DateTime>(field, mo);
        } else if(col.DataType == typeof(TimeSpan)) {
            Memory<TimeSpan?> mo = ((PrimitiveDataFrameColumn<TimeSpan>)col).ToArray().AsMemory();
            await rgw.WriteAsync<TimeSpan>(field, mo);
        } else if(col.DataType == typeof(decimal)) {
            Memory<decimal?> mo = ((PrimitiveDataFrameColumn<decimal>)col).ToArray().AsMemory();
            await rgw.WriteAsync<decimal>(field, mo);
        } else if(col.DataType == typeof(float)) {
            Memory<float?> mo = ((PrimitiveDataFrameColumn<float>)col).ToArray().AsMemory();
            await rgw.WriteAsync<float>(field, mo);
        } else if(col.DataType == typeof(double)) {
            Memory<double?> mo = ((PrimitiveDataFrameColumn<double>)col).ToArray().AsMemory();
            await rgw.WriteAsync<double>(field, mo);
        } else if(col.DataType == typeof(string)) {
            string[] mo = ((StringDataFrameColumn)col).ToArray();
            await rgw.WriteAsync(field, mo, null, null, CancellationToken.None);
        } else {
            throw new NotSupportedException($"unsupported column type {col.DataType}");
        }
    }
}
