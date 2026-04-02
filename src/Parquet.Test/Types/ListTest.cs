using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types;

public class ListTest : TestBase {
    [Fact]
    public async Task List_of_structures_writes_reads() {
        var nameField = new DataField<string>("name");
        var line1Field = new DataField<string>("line1");
        var postcodeField = new DataField<string>("postcode");

        var schema = new ParquetSchema(
           nameField,
           new ListField("addresses",
           new StructField(ListField.ElementName,
              line1Field,
              postcodeField)));

        string[] nameValues = ["Joe", "Bob"];
        string[] line1Values = ["Amazonland", "Disneyland", "Cryptoland"];
        string[] postcodeValues = ["AAABBB", "CCCDDD", "EEEFFF"];

        // write

        var ms = new MemoryStream();
        await using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms)) {
            using ParquetRowGroupWriter gw = w.CreateRowGroup();

            await gw.WriteAsync(nameField, nameValues);
            await gw.WriteAsync(line1Field, line1Values, new[] { 0, 1, 0 });
            await gw.WriteAsync(postcodeField, postcodeValues, new[] { 0, 1, 0 });
        }

        // read back
        ms.Position = 0;
        RawColumnData<ReadOnlyMemory<char>> names;
        RawColumnData<ReadOnlyMemory<char>> line1s;
        RawColumnData<ReadOnlyMemory<char>> postcodes;
        await using(ParquetReader r = await ParquetReader.CreateAsync(ms)) {
            using ParquetRowGroupReader gr = r.OpenRowGroupReader(0);
            names = await gr.ReadRawColumnDataAsync<ReadOnlyMemory<char>>(nameField);
            line1s = await gr.ReadRawColumnDataAsync<ReadOnlyMemory<char>>(line1Field);
            postcodes = await gr.ReadRawColumnDataAsync<ReadOnlyMemory<char>>(postcodeField);
        }

        // compare values and repetition/definition levels
        Assert.Equal(nameValues, names.ValuesAsStrings);
        Assert.Equal(line1Values, line1s.ValuesAsStrings);
        Assert.Equal(postcodeValues, postcodes.ValuesAsStrings);
        Assert.Equal([1, 1], names.DefinitionLevels);
        Assert.Equal([0, 1, 0], line1s.RepetitionLevels);
        Assert.Equal([0, 1, 0], postcodes.RepetitionLevels);

    }

    [Fact]
    public async Task List_of_elements_with_some_items_empty_reads_file() {
        /*
         list data (column 1):
         - 1: [1, 2, 3]
         - 2: []
         - 3: [1, 2, 3]
         - 4: []
         */

        await using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile("list_empty_alt.parquet")))

        using(ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0)) {
            Assert.Equal(4, groupReader.RowCount);
            DataField[] fs = reader.Schema.GetDataFields();

            using RawColumnData<int> idCol = await groupReader.ReadRawColumnDataAsync<int>(fs[0]);
            Assert.Equal(4, idCol.Values.Length);
            Assert.Equal([1, 2, 3, 4], idCol.Values);

            using RawColumnData<ReadOnlyMemory<char>> list = await groupReader.ReadRawColumnDataAsync<ReadOnlyMemory<char>>(fs[1]);
            Assert.Equal(8, list.Values.Length);
            //Assert.Equal([1, 2, 3, 1, 2, 3], list.Values);
            Assert.Equal([3, 3, 3, 1, 3, 3, 3, 1], list.DefinitionLevels);
            Assert.Equal([0, 1, 1, 0, 0, 1, 1, 0], list.RepetitionLevels);
        }

    }
}