using Parquet.Data;
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
         /*
          * This is a tricky one, as there are actually no elements in the second column, here is a dump of it:
          * 

repeats1.list.element TV=1 RL=1 DL=3                                         
---------------------------------------------------------------------------- 
page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:13 VC:1  
         
BINARY repeats1.list.element
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 1 ***
value 1: R:0 D:1 V:<null>
          *
          *  The dump shows there is actually one value, but with DL=1, whereas column's DL is 3. That means the list is created on level 1
          *  (repeats entry level).
          */

         DataSet ds = ParquetReader.Read(OpenTestFile("listofitems-empty-onerow.parquet"));
         Assert.Equal("{2;[]}", ds[0].ToString());
         Assert.Equal(1, ds.RowCount);
      }

      [Fact]
      public void List_of_elements_with_some_items_empty_reads_file()
      {
         DataSet ds = ParquetReader.Read(OpenTestFile("listofitems-empty-alternates.parquet"));
         Assert.Equal(4, ds.RowCount);
         Assert.Equal("{1;[1;2;3]}", ds[0].ToString());
         Assert.Equal("{2;[]}", ds[1].ToString());
         Assert.Equal("{3;[1;2;3]}", ds[2].ToString());
         Assert.Equal("{4;[]}", ds[3].ToString());
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
}