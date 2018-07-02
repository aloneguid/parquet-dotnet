/*using Parquet.Data;
using Parquet.File;
using Xunit;

namespace Parquet.Test
{
   public class ListTest : TestBase
   {
      [Fact]
      public void List_of_structures_writes_reads()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new ListField("cities",
            new StructField("element",
               new DataField<string>("name"),
               new DataField<string>("country"))));

         ds.Add(1, new[] { new Row("London", "UK"), new Row("New York", "US") });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{1;[{London;UK};{New York;US}]}", ds1[0].ToString());
      }

      [Fact]
      public void List_of_elements_writes_reads()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new ListField("strings",
               new DataField<string>("item")
            ));
         ds.Add(1, new[] { "one", "two" });
         Assert.Equal("{1;[one;two]}", ds.WriteReadFirstRow());
      }

      [Fact]
      public void List_of_elements_is_empty_reads_file()
      {

         DataSet ds = ParquetReader2.Read(OpenTestFile("listofitems-empty-onerow.parquet"));
         Assert.Equal("{2;[]}", ds[0].ToString());
         Assert.Equal(1, ds.RowCount);
      }

      [Fact]
      public void List_of_elements_with_some_items_empty_reads_file()
      {
         //v2
         DataSet ds = ParquetReader2.Read(OpenTestFile("listofitems-empty-alternates.parquet"));
         Assert.Equal(4, ds.RowCount);
         Assert.Equal("{1;[1;2;3]}", ds[0].ToString());
         Assert.Equal("{2;[]}", ds[1].ToString());
         Assert.Equal("{3;[1;2;3]}", ds[2].ToString());
         Assert.Equal("{4;[]}", ds[3].ToString());

         //v3
         using (var reader = new ParquetReader(OpenTestFile("listofitems-empty-alternates.parquet")))
         {
            using (ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0))
            {
               DataColumn id = groupReader.ReadColumn(reader.Schema.GetDataFields()[0]);
               DataColumn list = groupReader.ReadColumn(reader.Schema.GetDataFields()[1]);
            }
         }

      }

      [Fact]
      public void List_of_elements_is_empty_writes_reads()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new ListField("strings",
               new DataField<string>("item")
            ));
         ds.Add(1, new string[0]);
         Assert.Equal("{1;[]}", ds.WriteReadFirstRow());
      }

      [Fact]
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

      }
   }
}*/