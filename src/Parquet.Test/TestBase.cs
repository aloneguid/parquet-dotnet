using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using F = System.IO.File;

namespace Parquet.Test;

public class TestBase {

    protected Stream OpenTestFile(string name) {
        return F.OpenRead("./data/" + name);
    }

    protected StreamReader OpenTestFileReader(string name) {
        return new StreamReader("./data/" + name);
    }

    protected async Task<RawColumnData<T>?> WriteReadSingleColumn<T>(DataField df, ReadOnlyMemory<T> values, int[]? repetitionLevels = null) where T : struct {
        using var ms = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(df), ms)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(df, values, repetitionLevels);
            rg.CompleteValidate();
        }


        // write with built-in extension method
        ms.Position = 0;

        //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

        // read first gow group and first column
        await using ParquetReader reader = await ParquetReader.CreateAsync(ms);
        if(reader.RowGroupCount == 0)
            return null;
        ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

        return await rgReader.ReadRawColumnDataAsync<T>(df);
    }

    protected async Task<RawColumnData<T>?> WriteReadSingleColumn<T>(DataField df, ReadOnlyMemory<T?> values, int[]? repetitionLevels = null) where T : struct {
        using var ms = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(df), ms)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsync(df, values, repetitionLevels);
            rg.CompleteValidate();
        }

        // write with built-in extension method
        ms.Position = 0;

        //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

        // read first gow group and first column
        await using ParquetReader reader = await ParquetReader.CreateAsync(ms);
        if(reader.RowGroupCount == 0)
            return null;
        ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

        return await rgReader.ReadRawColumnDataAsync<T>(df);
    }

    protected async ValueTask<RawColumnData<T>> ReadColumn<T>(ParquetReader reader, DataField df) where T : struct {
        using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
        return await rgr.ReadRawColumnDataAsync<T>(df);
    }

    protected async ValueTask<List<string?>> ReadStringColumn(ParquetReader reader, DataField df) {
        RawColumnData<ReadOnlyMemory<char>> col = await ReadColumn<ReadOnlyMemory<char>>(reader, df);
        var r = new List<string?>();

        if(df.IsNullable) {
            // collapse
            int k = 0;
            for(int i = 0; i < col.DefinitionLevels.Length; i++) {
                bool isNull = col.DefinitionLevels[i] == 0;
                if(isNull) {
                    r.Add(null);
                } else {
                    r.Add(new string(col.Values[k++].Span));
                }
            }
        } else {
            for(int i = 0; i < col.Values.Length; i++) {
                r.Add(new string(col.Values[i].Span));
            }
        }
        return r;
    }

    protected static int GetValueBufferLength(ParquetRowGroupReader rgr, DataField field) {
        long numValues = rgr.GetMetadata(field)?.MetaData?.NumValues ?? rgr.RowCount;
        return checked((int)numValues);
    }

    protected static async Task<T[]> ReadValuesAsync<T>(ParquetRowGroupReader rgr, DataField field) where T : struct {
        if(field.MaxDefinitionLevel > 0 && field.MaxRepetitionLevel == 0) {
            T?[] nullableValues = await ReadNullableValuesAsync<T>(rgr, field);
            return nullableValues.Where(v => v.HasValue).Select(v => v!.Value).ToArray();
        }

        int bufferLength = GetValueBufferLength(rgr, field);
        T[] values = new T[bufferLength];
        Memory<int>? repetitionLevels = null;
        if(field.MaxRepetitionLevel > 0) {
            repetitionLevels = new int[bufferLength].AsMemory();
        }
        await rgr.ReadAsync<T>(field, values.AsMemory(), repetitionLevels);
        return values;
    }

    protected static async Task<T?[]> ReadNullableValuesAsync<T>(ParquetRowGroupReader rgr, DataField field) where T : struct {
        T?[] values = new T?[checked((int)rgr.RowCount)];
        int repetitionLevelLength = GetValueBufferLength(rgr, field);
        Memory<int>? repetitionLevels = null;
        if(field.MaxRepetitionLevel > 0) {
            repetitionLevels = new int[repetitionLevelLength].AsMemory();
        }
        await rgr.ReadAsync<T>(field, values.AsMemory(), repetitionLevels);
        return values;
    }

    protected static async Task<string?[]> ReadStringValuesAsync(ParquetRowGroupReader rgr, DataField field) {
        if(field.MaxRepetitionLevel > 0) {
            int bufferLength = GetValueBufferLength(rgr, field);
            ReadOnlyMemory<char>?[] repeatedValues = new ReadOnlyMemory<char>?[bufferLength];
            Memory<int>? repetitionLevels = new int[bufferLength].AsMemory();
            await rgr.ReadAsync<ReadOnlyMemory<char>>(field, repeatedValues.AsMemory(), repetitionLevels);
            return repeatedValues.Select(x => x.HasValue ? new string(x.Value.Span) : null).ToArray();
        }

        ReadOnlyMemory<char>?[] values = new ReadOnlyMemory<char>?[checked((int)rgr.RowCount)];
        await rgr.ReadAsync<ReadOnlyMemory<char>>(field, values.AsMemory(), null);
        return values.Select(v => v.HasValue ? new string(v.Value.Span) : null).ToArray();
    }

    protected static async Task<byte[][]> ReadBinaryValuesAsync(ParquetRowGroupReader rgr, DataField field) {
        if(field.MaxDefinitionLevel > 0 && field.MaxRepetitionLevel == 0) {
            ReadOnlyMemory<byte>?[] values = new ReadOnlyMemory<byte>?[checked((int)rgr.RowCount)];
            await rgr.ReadAsync<ReadOnlyMemory<byte>>(field, values.AsMemory(), null);
            return values.Where(v => v.HasValue).Select(v => v!.Value.ToArray()).ToArray();
        }

        int bufferLength = GetValueBufferLength(rgr, field);
        ReadOnlyMemory<byte>[] binaryValues = new ReadOnlyMemory<byte>[bufferLength];
        Memory<int>? repetitionLevels = null;
        if(field.MaxRepetitionLevel > 0) {
            repetitionLevels = new int[bufferLength].AsMemory();
        }
        await rgr.ReadAsync<ReadOnlyMemory<byte>>(field, binaryValues.AsMemory(), repetitionLevels);
        return binaryValues.Select(v => v.ToArray()).ToArray();
    }

    protected static Task ReadAnyFieldAsync(ParquetRowGroupReader rgr, DataField field) {
        if(field.ClrType == typeof(string)) {
            return ReadStringValuesAsync(rgr, field);
        }

        if(field.ClrType == typeof(byte[])) {
            return ReadBinaryValuesAsync(rgr, field);
        }

        MethodInfo method = typeof(TestBase)
            .GetMethod(nameof(ReadAnyStructFieldAsync), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(field.ClrType);

        return (Task)method.Invoke(null, new object[] { rgr, field })!;
    }

    private static async Task ReadAnyStructFieldAsync<T>(ParquetRowGroupReader rgr, DataField field) where T : struct {
        if(field.MaxDefinitionLevel > 0 && field.MaxRepetitionLevel == 0) {
            await ReadNullableValuesAsync<T>(rgr, field);
            return;
        }

        await ReadValuesAsync<T>(rgr, field);
    }
}