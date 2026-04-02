using System;
using System.Collections.Generic;
using System.IO;
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

    protected async Task<DataColumn?> WriteReadSingleColumn<T>(DataField df, ReadOnlyMemory<T> values, int[]? repetitionLevels = null) where T : struct {
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

        return await rgReader.ReadColumnAsync(df);
    }

    protected async Task<DataColumn?> WriteReadSingleColumn<T>(DataField df, ReadOnlyMemory<T?> values, int[]? repetitionLevels = null) where T : struct {
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

        return await rgReader.ReadColumnAsync(df);
    }

    protected async Task<List<DataColumn>> ReadColumns(Stream s) {
        await using ParquetReader reader = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
        var r = new List<DataColumn>();
        foreach(DataField df in reader.Schema.DataFields) {
            DataColumn dc = await rgr.ReadColumnAsync(df);
            r.Add(dc);
        }
        return r;
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
}