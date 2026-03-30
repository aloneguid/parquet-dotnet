using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types;

public class RepeatableFieldsTest : TestBase {

    [Fact]
    public async Task Simple_repeated_field_write_read() {
        // arrange 
        var schema = new ParquetSchema(new DataField<IEnumerable<int>>("items"));

        int[] values = [1, 2, 3, 4, 5];
        int[] repetitionLevels = [0, 1, 1, 0, 1];

        var column = new DataColumn(
           schema.GetDataFields()[0],
           values,
           repetitionLevels);

        // act
        DataColumn? rc = await WriteReadSingleColumn(column.Field, values, repetitionLevels);

        // assert
        Assert.Equal(new int[] { 1, 2, 3, 4, 5 }, rc!.Data);
        Assert.Equal(new int[] { 0, 1, 1, 0, 1 }, rc!.RepetitionLevels);
    }

    [Fact]
    public async Task Nullable_repeated_field_write_read() {
        // arrange 
        var schema = new ParquetSchema(new DataField<IEnumerable<int?>>("items"));

        int?[] values = [1, 2, null, 4, 5];
        int[] repetitionLevels = [0, 1, 1, 0, 1];

        var column = new DataColumn(
           schema.DataFields[0],
           values, repetitionLevels);

        // act
        DataColumn? rc = await WriteReadSingleColumn(column.Field, values, repetitionLevels);

        // assert
        Assert.Equal(values, rc!.Data);
        Assert.Equal(repetitionLevels, rc!.RepetitionLevels);
    }

    [Fact]
    public async Task Nullable1_repeated_field_write_read() {
        // arrange 
        var schema = new ParquetSchema(new DataField<IEnumerable<int>>("items") { IsNullable = true });
        int?[] values = { 1, 2, 3, 4, 5 };
        int[] repetitionLevels = { 0, 1, 1, 0, 1 };
        var column = new DataColumn(
           schema.GetDataFields()[0],
           values,
           repetitionLevels);

        // act
        DataColumn? rc = await WriteReadSingleColumn(column.Field, values, repetitionLevels);

        // assert
        Assert.Equal(values, rc!.Data);
        Assert.Equal(repetitionLevels, rc!.RepetitionLevels);
    }
}
