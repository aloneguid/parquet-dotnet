using System;
using System.Collections.Generic;
using Parquet.Data;
using Parquet.File;
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

         DataColumn[] readColumns = WriteReadSingleRowGroup(schema, new[] { id, cname, ccountry }, 1, out Schema readSchema);

      }

      /*[Fact]
      public void List_of_elements_writes_reads()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new ListField("strings",
               new DataField<string>("item")
            ));
         ds.Add(1, new[] { "one", "two" });
         Assert.Equal("{1;[one;two]}", ds.WriteReadFirstRow());
      }*/

      /*[Fact]
      public void List_of_elements_is_empty_reads_file()
      {

         DataSet ds = ParquetReader2.Read(OpenTestFile("listofitems-empty-onerow.parquet"));
         Assert.Equal("{2;[]}", ds[0].ToString());
         Assert.Equal(1, ds.RowCount);
      }*/

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

      /*[Fact]
      public void List_of_elements_is_empty_writes_reads()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new ListField("strings",
               new DataField<string>("item")
            ));
         ds.Add(1, new string[0]);
         Assert.Equal("{1;[]}", ds.WriteReadFirstRow());
      }*/

      /*[Fact]
      public void List_of_elements_with_some_items_empty_writes_reads()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new ListField("strings",
               new DataField<string>("item")
            ));
         ds.Add(1, new string[] { "1", "2", "3" });
         ds.Add(2, new string[] { });
         ds.Add(3, new string[] { "1", "2", "3" });
         ds.Add(4, new string[] { });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);
         Assert.Equal(4, ds1.RowCount);
         Assert.Equal("{1;[1;2;3]}", ds1[0].ToString());
         Assert.Equal("{2;[]}", ds1[1].ToString());
         Assert.Equal("{3;[1;2;3]}", ds1[2].ToString());
         Assert.Equal("{4;[]}", ds1[3].ToString());

      }*/
   }
}