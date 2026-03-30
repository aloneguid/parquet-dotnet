using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Analysis;
using Parquet.Encodings;
using Parquet.Schema;
using Xunit;
using System.Linq;
using Parquet.Data;

namespace Parquet.Test.DataAnalysis;

public class DataFrameReaderTest : TestBase {

    [Fact]
    public async Task Roundtrip_short() {
        await RoundtripAllTypesCore((short)1, (short)2);
    }

    [Fact]
    public async Task Roundtrip_nullable_short_with_null() {
        await RoundtripAllTypesNullableCore<short>(null, (short)2);
    }

    [Fact]
    public async Task Roundtrip_int() {
        await RoundtripAllTypesCore(1, 2);
    }

    [Fact]
    public async Task Roundtrip_nullable_int_with_null() {
        await RoundtripAllTypesNullableCore<int>(null, 2);
    }

    [Fact]
    public async Task Roundtrip_bool() {
        await RoundtripAllTypesCore(true, false);
    }

    [Fact]
    public async Task Roundtrip_nullable_bool_with_null() {
        await RoundtripAllTypesNullableCore<bool>(true, null);
    }

    [Fact]
    public async Task Roundtrip_long() {
        await RoundtripAllTypesCore(1L, 2L);
    }

    [Fact]
    public async Task Roundtrip_nullable_long() {
        await RoundtripAllTypesNullableCore<long>(1L, 2L);
    }

    [Fact]
    public async Task Roundtrip_ulong() {
        await RoundtripAllTypesCore(1UL, 2UL);
    }

    [Fact]
    public async Task Roundtrip_nullable_ulong() {
        await RoundtripAllTypesNullableCore<ulong>(1UL, 2UL);
    }

    [Fact]
    public async Task Roundtrip_string() {
        await RoundtripStringCore("1", "2");
    }

    [Fact]
    public async Task Roundtrip_string_with_null() {
        await RoundtripStringCore(null, "2");
    }

    private async Task RoundtripAllTypesCore<T>(T el1, T el2) where T : struct {
        using var ms = new MemoryStream();
        T[] data = [el1, el2];

        var field = new DataField(typeof(T).Name, typeof(T));
        var schema = new ParquetSchema(field);

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
            using ParquetRowGroupWriter rgw = writer.CreateRowGroup();
            await rgw.WriteAsync<T>(field, data.AsMemory());
        }

        await AssertRoundtripDataFrameCore(ms, field.Name, typeof(T));
    }

    private async Task RoundtripAllTypesNullableCore<T>(T? el1, T? el2) where T : struct {
        using var ms = new MemoryStream();
        T?[] data = [el1, el2];

        var field = new DataField(typeof(T?).Name, typeof(T?));
        var schema = new ParquetSchema(field);

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
            using ParquetRowGroupWriter rgw = writer.CreateRowGroup();
            await rgw.WriteAsync<T>(field, data.AsMemory());
        }

        await AssertRoundtripDataFrameCore(ms, field.Name, typeof(T?));
    }

    private async Task RoundtripStringCore(string? el1, string? el2) {
        using var ms = new MemoryStream();
        string?[] data = [el1, el2];

        var field = new DataField(typeof(string).Name, typeof(string));
        var schema = new ParquetSchema(field);

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
            using ParquetRowGroupWriter rgw = writer.CreateRowGroup();
            await rgw.WriteAsync<ReadOnlyMemory<char>>(field, data.Select(x => x.AsReadOnlyMemory()).ToArray());
        }

        await AssertRoundtripDataFrameCore(ms, field.Name, typeof(string));
    }

    private static async Task AssertRoundtripDataFrameCore(Stream ms, string fieldName, Type type) {
        ms.Position = 0;
        DataFrame df = await ms.ReadParquetAsDataFrameAsync();

        using var ms1 = new MemoryStream();
        await df.WriteAsync(ms1);

        ms1.Position = 0;
        DataFrame df1 = await ms1.ReadParquetAsDataFrameAsync();

        if(type == typeof(long)) {
            // Int64 is a special case in DataFrame
            // see https://github.com/aloneguid/parquet-dotnet/issues/343 for more info
            df1.Columns.GetInt64Column(fieldName);
        } else if(type == typeof(ulong)) {
            df1.Columns.GetUInt64Column(fieldName);
        }

        Assert.Equal(df.Columns.Count, df1.Columns.Count);
        for(int i = 0; i < df.Columns.Count; i++) {
            Assert.Equal(df.Columns[i], df1.Columns[i]);
        }
    }

    [Fact]
    public async Task Read_postcodes_file() {
        using Stream fs = OpenTestFile("postcodes.plain.parquet");
        DataFrame df = await fs.ReadParquetAsDataFrameAsync();
    }

    [Fact]
    public async Task Read_nested_file() {
        using Stream fs = OpenTestFile("simplenested.parquet");
        DataFrame df = await fs.ReadParquetAsDataFrameAsync();
    }

    [Fact]
    public async Task Read_multiple_row_groups() {
        // generate file with multiple row groups

        var ms = new MemoryStream();
        var id = new DataField<int>("id");

        //write
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new[] { 1, 2, 3, 4 });
            }

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new[] { 5, 6 });
            }
        }

        // read as DataFrame
        ms.Position = 0;
        DataFrame df = await ms.ReadParquetAsDataFrameAsync();

        // check that all the values are present
        Assert.Equal(6, df.Rows.Count);
        Assert.Equal(1, df.Rows[0][0]);
        Assert.Equal(2, df.Rows[1][0]);
    }
}
