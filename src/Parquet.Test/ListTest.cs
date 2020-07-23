using System.Threading.Tasks;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class ListTest : TestBase
   {
      [Fact]
      public async Task List_of_structures_writes_readsAsync()
      {
         var idsch = new DataField<int>("id");
         var cnamech = new DataField<string>("name");
         var ccountrych = new DataField<string>("country");

         var schema = new Schema(
            idsch,
            new ListField("cities",
            new StructField("element",
               cnamech,
               ccountrych)));

         var id = new DataColumn(idsch, new int[] { 1 });
         var cname = new DataColumn(cnamech, new[] { "London", "New York" }, new[] { 0, 1 });
         var ccountry = new DataColumn(ccountrych, new[] { "UK", "US" }, new[] { 0, 1 });

         await WriteReadSingleRowGroupAsync(schema, new[] { id, cname, ccountry }).ConfigureAwait(false);
      }

      [Fact]
      public async Task List_of_elements_with_some_items_empty_reads_fileAsync()
      {
         /*
          list data:
          - 1: [1, 2, 3]
          - 2: []
          - 3: [1, 2, 3]
          - 4: []
          */

         await using (var reader = new ParquetReader(OpenTestFile("list_empty_alt.parquet")))
         {
            using (ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(4, groupReader.RowCount);
               DataField[] fs = reader.Schema.GetDataFields();

               DataColumn id = await groupReader.ReadColumnAsync(fs[0]).ConfigureAwait(false);
               Assert.Equal(4, id.Data.Length);
               Assert.False(id.HasRepetitions);

               DataColumn list = await groupReader.ReadColumnAsync(fs[1]).ConfigureAwait(false);
               Assert.Equal(8, list.Data.Length);
               Assert.Equal(new int[] { 0, 1, 1, 0, 0, 1, 1, 0 }, list.RepetitionLevels);
            }
         }

      }
   }
}