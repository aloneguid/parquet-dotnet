using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class ListTest : TestBase
   {
      [Fact]
      public void List_of_structures_writes_reads()
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

         DataColumn[] readColumns = WriteReadSingleRowGroup(schema, new[] { id, cname, ccountry }, out Schema readSchema);

      }

      [Fact]
      public void List_of_elements_with_some_items_empty_reads_file()
      {
         /*
          list data:
          - 1: [1, 2, 3]
          - 2: []
          - 3: [1, 2, 3]
          - 4: []
          */

         using (var reader = new ParquetReader(OpenTestFile("listofitems-empty-alternates.parquet")))
         {
            using (ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(4, groupReader.RowCount);
               DataField[] fs = reader.Schema.GetDataFields();

               DataColumn id = groupReader.ReadColumn(fs[0]);
               Assert.Equal(4, id.Data.Length);
               Assert.False(id.HasRepetitions);

               DataColumn list = groupReader.ReadColumn(fs[1]);
               Assert.Equal(8, list.Data.Length);
               Assert.Equal(new int[] { 0, 1, 1, 0, 0, 1, 1, 0 }, list.RepetitionLevels);
            }
         }

      }
   }
}