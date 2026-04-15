using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test;

public class DictionaryEncodingTest : TestBase {

    [Fact]
    public async Task DictionaryEncodingTest2() {
        string?[] data = [
            "xxx",
            "xxx",
            "xxx",
            "xxx",
            "xxx",
            null,
            "yyy",
            "yyy",
            "yyy",
            string.Empty,
            "yyy",
            "yyy",
            null,
            null,
            "zzz",
            "zzz",
            "zzz",
            "zzz"
            ];

        var dataField = new DataField<string>("string");
        var parquetSchema = new ParquetSchema(dataField);

        using var stream = new MemoryStream();

        var options = new ParquetOptions();
        options.DictionaryEncodedColumns.Add("string");
        await using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, stream, formatOptions: options)) {
            using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
            await groupWriter.WriteAsync<ReadOnlyMemory<char>>(dataField, data.Select(x => x.AsNullableReadOnlyMemory()).ToArray());
        }

        await using ParquetReader parquetReader = await ParquetReader.CreateAsync(stream);
        List<string?> rdata = await ReadStringColumn(parquetReader, dataField);

        Assert.Equal(data, rdata);
    }

    [Fact]
    public async Task ReadStringDictionaryGeneratedBySpark() {
        using Stream fs = OpenTestFile("string_dictionary_by_spark.parquet");
        await using ParquetReader reader = await ParquetReader.CreateAsync(fs);
        List<string?> values = await ReadStringColumn(reader, reader.Schema.DataFields[0]);

        Assert.Equal(400, values.Count);
        Assert.Equal(Enumerable.Repeat("one", 100).ToArray(), values.Take(100));
        Assert.Equal(Enumerable.Repeat("two", 100).ToArray(), values.Skip(100).Take(100));
        Assert.Equal(Enumerable.Repeat((string?)null, 100).ToArray(), values.Skip(200).Take(100));
        Assert.Equal(Enumerable.Repeat("three", 100).ToArray(), values.Skip(300).Take(100));
    }
}