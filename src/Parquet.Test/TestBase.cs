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
}