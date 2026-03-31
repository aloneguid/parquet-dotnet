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

        string[] nameValues = new string[] { "Joe", "Bob" };
        string[] line1Values = new[] { "Amazonland", "Disneyland", "Cryptoland" };
        string[] postcodeValues = new[] { "AAABBB", "CCCDDD", "EEEFFF" };

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
        DataColumn dcNames;
        DataColumn dcLine1;
        DataColumn dcPostcode;
        await using(ParquetReader r = await ParquetReader.CreateAsync(ms)) {
            using ParquetRowGroupReader gr = r.OpenRowGroupReader(0);
            dcNames = await gr.ReadColumnAsync(nameField);
            dcLine1 = await gr.ReadColumnAsync(line1Field);
            dcPostcode = await gr.ReadColumnAsync(postcodeField);
        }

        // compare values and repetition/definition levels
        Assert.Equal(nameValues, dcNames.Data);
        Assert.Equal(line1Values, dcLine1.Data);
        Assert.Equal(postcodeValues, dcPostcode.Data);
        Assert.Equal(new int[] { 1, 1 }, dcNames.DefinitionLevels);
        Assert.Equal(new int[] { 0, 1, 0 }, dcLine1.RepetitionLevels);
        Assert.Equal(new int[] { 0, 1, 0 }, dcPostcode.RepetitionLevels);

    }

    [Fact]
    public async Task List_of_elements_with_some_items_empty_reads_file() {
        /*
         list data:
         - 1: [1, 2, 3]
         - 2: []
         - 3: [1, 2, 3]
         - 4: []
         */

        await using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile("list_empty_alt.parquet")))

        using(ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0)) {
            Assert.Equal(4, groupReader.RowCount);
            DataField[] fs = reader.Schema.GetDataFields();

            DataColumn id = await groupReader.ReadColumnAsync(fs[0]);
            Assert.Equal(4, id.Data.Length);
            Assert.False(id.HasRepetitions);

            DataColumn list = await groupReader.ReadColumnAsync(fs[1]);
            Assert.Equal(8, list.Data.Length);
            Assert.Equal(new int[] { 3, 3, 3, 1, 3, 3, 3, 1 }, list.DefinitionLevels);
            Assert.Equal(new int[] { 0, 1, 1, 0, 0, 1, 1, 0 }, list.RepetitionLevels);
        }

    }
}