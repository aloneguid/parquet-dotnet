using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types {
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

            var nameCol = new DataColumn(nameField, new string[] { "Joe", "Bob" });
            var line1Col = new DataColumn(line1Field, new[] { "Amazonland", "Disneyland", "Cryptoland" }, new[] { 0, 1, 0 });
            var postcodeCol = new DataColumn(postcodeField, new[] { "AAABBB", "CCCDDD", "EEEFFF" }, new[] { 0, 1, 0 });

            await WriteReadSingleRowGroup(schema, new[] { nameCol, line1Col, postcodeCol });
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

            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile("list_empty_alt.parquet")))

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
}