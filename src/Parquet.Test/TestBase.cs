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

    protected async Task<DataColumn?> WriteReadSingleColumn(DataField df, Array data, int[]? repetitionLevels = null) {
        using var ms = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(df), ms)) {
            using ParquetRowGroupWriter rg = writer.CreateRowGroup();
            await rg.WriteAsyncV1(df, data, repetitionLevels);
            rg.CompleteValidate();
        }


        // write with built-in extension method
        ms.Position = 0;

        //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

        // read first gow group and first column
        using ParquetReader reader = await ParquetReader.CreateAsync(ms);
        if(reader.RowGroupCount == 0)
            return null;
        ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

        return await rgReader.ReadColumnAsync(df);
    }

    protected async Task<object> WriteReadSingle(DataField field, object? value, CompressionMethod compressionMethod = CompressionMethod.None) {
        //for sanity, use disconnected streams
        byte[] data;

        var options = new ParquetOptions();
        if(value is DateOnly)
            options.UseDateOnlyTypeForDates = true;

        using(var ms = new MemoryStream()) {
            // write single value

            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), ms, options)) {
                writer.CompressionMethod = compressionMethod;

                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                dataArray.SetValue(value, 0);
                await rg.WriteAsyncV1(field, dataArray);
            }

            data = ms.ToArray();
        }

        using(var ms = new MemoryStream(data)) {
            // read back single value

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataColumn column = await rowGroupReader.ReadColumnAsync(field);

            return column.Data.GetValue(0)!;
        }
    }

    protected async Task<List<DataColumn>> ReadColumns(Stream s) {
        using ParquetReader reader = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
        var r = new List<DataColumn>();
        foreach(DataField df in reader.Schema.DataFields) {
            DataColumn dc = await rgr.ReadColumnAsync(df);
            r.Add(dc);
        }
        return r;
    }
}