using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test;

/// <summary>
/// Tests a set of predefined test files that they read back correct. Find more test data (some taken from there):
/// https://github.com/apache/parquet-testing/tree/master/data
/// </summary>
public class ParquetReaderOnTestFilesTest : TestBase {

    [Theory]
    [InlineData("fixedlenbytearray.parquet")]
    [InlineData("fixedlenbytearray.v2.parquet")]
    public async Task FixedLenByteArray_dictionary(string parquetFile) {
        await using Stream s = OpenTestFile(parquetFile);
        await using ParquetReader r = await ParquetReader.CreateAsync(s);

        ParquetSerializer.UntypedResult result = await ParquetSerializer.DeserializeAsync(s);
    }

    [Theory]
    [InlineData("dates.parquet")]
    [InlineData("dates.v2.parquet")]
    public async Task Datetypes_all(string parquetFile) {
        using Stream s = OpenTestFile(parquetFile);

        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        var col0 = new DateTime?[rgr.RowGroup.NumRows];
        var col1 = new DateTime?[rgr.RowGroup.NumRows];
        await rgr.ReadAsync<DateTime>(r.Schema.DataFields[0], col0);
        await rgr.ReadAsync<DateTime>(r.Schema.DataFields[1], col1);

        DateTime? o0 = col0[0];
        DateTime? o1 = col0[1];

        Assert.Equal(new DateTime(2017, 1, 1), o0);
        Assert.Equal(new DateTime(2017, 2, 1), o1);
    }

    [Theory]
    [InlineData("datetime_other_system.parquet")]
    [InlineData("datetime_other_system.v2.parquet")]
    public async Task DateTime_FromOtherSystem(string parquetFile) {
        using(Stream s = OpenTestFile(parquetFile)) {
            await using(ParquetReader r = await ParquetReader.CreateAsync(s)) {

                using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

                DataField? field = r.Schema.GetDataFields().FirstOrDefault(x => x.Name == "as_at_date_");
                Assert.NotNull(field);

                var col = new DateTime?[rgr.RowGroup.NumRows];
                await rgr.ReadAsync<DateTime>(field, col);

                DateTime? offset = col[0];
                Assert.Equal(new DateTime(2018, 12, 14, 0, 0, 0), offset!.Value.Date);
            }
        }
    }

    private async Task OptionalValues_WithoutStatistics(string parquetFile) {
        using(Stream s = OpenTestFile(parquetFile)) {
            await using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

                DataField? idField = r.Schema.GetDataFields().FirstOrDefault(x => x.Name == "id");
                DataField? valueField = r.Schema.GetDataFields().FirstOrDefault(x => x.Name == "value");
                Assert.NotNull(idField);
                Assert.NotNull(valueField);

                long?[] ids = await ReadNullableValuesAsync<long>(rgr, idField);
                int?[] values = await ReadNullableValuesAsync<int>(rgr, valueField);

                int index = Enumerable.Range(0, ids.Length)
                    .First(i => ids[i] == 20908539289);

                Assert.Equal(0, values[index]);
            }
        }
    }

    [Theory]
    [InlineData("issue-164.parquet")]
    [InlineData("issue-164.v2.parquet")]
    public async Task Issue164(string parquetFile) {
        using(Stream s = OpenTestFile(parquetFile)) {
            await using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);
                DataField[] fields = r.Schema.GetDataFields();

                int?[] ids = await ReadNullableValuesAsync<int>(rgr, fields[0]);

                Assert.Contains(256779, ids);
            }
        }
    }

    [Fact]
    public async Task ByteArrayDecimal() {
        using Stream s = OpenTestFile("byte_array_decimal.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);

        ParquetSchema schema = r.Schema;
        Assert.Equal("value", schema[0].Name);
        Assert.Single(schema.GetDataFields());

        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);
        decimal?[] data = await ReadNullableValuesAsync<decimal>(rgr, schema.GetDataFields()[0]);

        Assert.Equal(
            Enumerable.Range(1, 24).Select(i => (decimal?)i),
            data);
    }

    [Fact]
    public async Task Read_delta_binary_packed() {
        using Stream s = OpenTestFile("delta_binary_packed.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);

        ParquetSchema schema = r.Schema;

        using(ParquetRowGroupReader rgr = r.OpenRowGroupReader(0)) {
            DataField[] dfs = schema.GetDataFields();
            long[] values = await ReadValuesAsync<long>(rgr, dfs[1]);
            Assert.Equal(200, values.Length);
        }
    }


    [Fact]
    public async Task Read_legacy_list() {
        using Stream s = OpenTestFile("special/legacy-list.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Equal(3, fields.Length);

        string?[] col0 = await ReadStringValuesAsync(rgr, fields[0]);
        double[] col1 = await ReadValuesAsync<double>(rgr, fields[1]);

        Assert.Equal(new string[] { "1_0", "1_0" }, col0);
        Assert.Equal(new double[] { 2004, 2004 }, col1);
        Assert.Equal(336, GetValueBufferLength(rgr, fields[2]));
    }

    [Fact]
    public async Task Read_empty_and_null_lists() {
        using Stream s = OpenTestFile("list_empty_and_null.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Equal(2, fields.Length);
    }

    [Fact]
    public async Task Wide() {
        using Stream s = OpenTestFile("special/wide.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        foreach(DataField field in r.Schema.GetDataFields()) {
            await ReadAnyFieldAsync(rgr, field);
        }
    }

    [Fact]
    public async Task Oracle_Int64_Field_With_Extra_Byte() {
        using Stream s = OpenTestFile("oracle_int64_extra_byte_at_end.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Equal(2, fields.Length);

        string[] col0 = (await ReadStringValuesAsync(rgr, fields[0])).Select(x => x!).ToArray();
        long[] col1 = await ReadValuesAsync<long>(rgr, fields[1]);

        Assert.Equal(126, col0.Length);
        Assert.Equal(126, col1.Length);
        Assert.Equal("DEPOSIT", col0[0]);
        Assert.Equal("DEPOSIT", col0[125]);
        Assert.Equal((long)1, col1[0]);
        Assert.Equal((long)1, col1[125]);
    }

    [Fact]
    public async Task FixedLenByteArrayWithDictTest() {
        using Stream s = OpenTestFile("fixed_len_byte_array_with_dict.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Equal(10, fields.Length);

        // last column is a dictionary-encoded FIXED_LEN_BYTE_ARRAY.
        byte[][] data = await ReadBinaryValuesAsync(rgr, fields[9]);
        Assert.Equal(6, data.Length);
        Assert.Equal(Encoding.ASCII.GetBytes("abc"), data[0]);
        Assert.Equal(Encoding.ASCII.GetBytes("def"), data[1]);
        Assert.Equal(Encoding.ASCII.GetBytes("ghi"), data[2]);
        Assert.Equal(Encoding.ASCII.GetBytes("jkl"), data[3]);
        Assert.Equal(Encoding.ASCII.GetBytes("mno"), data[4]);
        Assert.Equal(Encoding.ASCII.GetBytes("qrs"), data[5]);
    }

    [Fact]
    public async Task GuidEndianTest() {
        using Stream s = OpenTestFile("cetas4.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Single(fields);

        Guid?[] data = await ReadNullableValuesAsync<Guid>(rgr, fields[0]);
        Assert.Equal(Guid.Parse("15A2501E-4899-4FF8-AF51-A1805FE0718F"), data[0]);
    }

    [Fact]
    public async Task ThriftProtocolBreakingChangeJune2024() {
        using Stream s = OpenTestFile("thrift/breaking-spec-2024.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Equal(55, fields.Length);

        await ReadAnyFieldAsync(rgr, fields[0]);
    }

    [Fact]
    public async Task ThriftProtocolBreakingChangeJune2024_Untyped() {
        using Stream s = OpenTestFile("thrift/breaking-spec-2024.parquet");
        ParquetSerializer.UntypedResult r = await ParquetSerializer.DeserializeAsync(s);
    }

    [Fact]
    public async Task DecimalsWithNoDefinedScale() {
        using Stream s = OpenTestFile("decimals_with_precision_but_no_scale.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Equal(8, fields.Length);

        //DECIMAL(9, 5)
        decimal?[] data = await ReadNullableValuesAsync<decimal>(rgr, fields[5]);
        Assert.Equal(1.02232M, data[7]);
        Assert.Equal(-27.427M, data[344]);

        //DECIMAL(9, 0)
        data = await ReadNullableValuesAsync<decimal>(rgr, fields[6]);
        Assert.Equal(0M, data[0]);
        Assert.Equal(1000M, data[6616]);
    }

    [Fact]
    public async Task DuckDbRLE_637() {
        using Stream s = OpenTestFile("issues/637-duckdb.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        Assert.Single(fields);

        int?[] data = await ReadNullableValuesAsync<int>(rgr, fields[0]);
        Assert.Equal(new int?[] { 2023, 2024 }, data);
    }

    [Fact]
    public async Task HyparquetCompressed() {
        using Stream s = OpenTestFile("hyparquet.snappy.parquet");
        ParquetSerializer.UntypedResult r = await ParquetSerializer.DeserializeAsync(s);
    }

    [Fact]
    public async Task NestedGroup_681() {
        using Stream s = OpenTestFile("issues/681_nested.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField df = r.Schema.FindDataField(new FieldPath("data_group", "nested"));

        string?[] values = await ReadStringValuesAsync(rgr, df);

        Assert.Single(values);
    }

    [Fact]
    public async Task BigDecimalDefaultOptions() {
        using Stream s = OpenTestFile("bigdecimal.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        DataField[] fields = r.Schema.GetDataFields();
        await Assert.ThrowsAsync<OverflowException>(async () => {
            foreach(DataField field in fields) {
                await ReadAnyFieldAsync(rgr, field);
            }
        });
    }

    [Fact]
    public async Task BigDecimalWithUseBigDecimalsOptionOn() {
        using Stream s = OpenTestFile("bigdecimal.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s, new ParquetOptions {
            UseBigDecimal = true
        });
        using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);

        foreach(DataField field in r.Schema.GetDataFields()) {
            await ReadAnyFieldAsync(rgr, field);
        }
    }

    [Fact]
    public async Task PyArrow22() {
        using Stream s = OpenTestFile("special/pyarrow_v22.parquet");
        await using ParquetReader r = await ParquetReader.CreateAsync(s);

        using ParquetRowGroupReader groupReader = r.OpenRowGroupReader(0);

        Assert.Equal(4626, groupReader.RowCount);
        DataField[] fs = r.Schema.GetDataFields();
        Assert.Equal(2, fs.Length);

        TimeSpan?[] data = await ReadNullableValuesAsync<TimeSpan>(groupReader, fs[1]);
        Assert.Equal(TimeSpan.FromTicks(215720000000), data[0]);
    }
}
