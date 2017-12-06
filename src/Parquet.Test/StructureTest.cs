using System.Collections.Generic;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class StructureTest
   {
      [Fact]
      public void Simple_structure_write_read()
      {
         var ds = new DataSet(
            new DataField<string>("name"),
            new StructField("address",
               new DataField<string>("line1"),
               new DataField<string>("postcode")
            ));

         ds.Add("Ivan", new Row("Woods", "postcode"));
         Assert.Equal(1, ds.RowCount);
         Assert.Equal(2, ds.FieldCount);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{Ivan;{Woods;postcode}}", ds1[0].ToString());
      }


      [Fact]
      public void Structure_nested_into_structure_write_read()
      {
         var ds = new DataSet(
            new DataField<string>("name"),
            new StructField("address",
               new DataField<string>("name"),
               new StructField("lines",
                  new DataField<string>("line1"),
                  new DataField<string>("line2"))));

         ds.Add("Ivan", new Row("Primary", new Row("line1", "line2")));

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{Ivan;{Primary;{line1;line2}}}", ds1[0].ToString());
      }

      [Fact]
      public void Structure_with_three_level_nesting()
      {
         var ds = new DataSet(
            new DataField<string>("name"),
            new StructField("address",
               new DataField<string>("name"),
               new StructField("lines",
                  new DataField<string>("line1"),
                  new StructField("line2",
                     new DataField<string>("here")))));
         ds.Add("Ivan", new Row("Primary", new Row("line1", new Row("here"))));
         Assert.Equal("{Ivan;{Primary;{line1;{here}}}}", ds.WriteReadFirstRow());
      }

      [Fact]
      public void Structure_with_repeated_field_writes_reads()
      {
         var ds = new DataSet(
            new DataField<string>("name"),
            new StructField("address",
               new DataField<string>("name"),
               new DataField<IEnumerable<string>>("lines")));

         ds.Add("Ivan", new Row("Primary", new[] { "line1", "line2" }));

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{Ivan;{Primary;[line1;line2]}}", ds1[0].ToString());
      }

      [Fact]
      public void Structure_in_a_list_in_a_structure_of_lists_writes_reads()
      {
         var ds = new DataSet(
            new DataField<string>("name"),
            new ListField("addresses",
               new StructField("address",
                  new ListField("lines",
                     new StructField("first", new DataField<int>("one"))))));

         ds.Add(
            "Ivan",           // name
            new Row[]         // addresses
            {
               new Row        // addresses.address
               (
                  true,
                  new Row[]   // addresses.address.lines
                  {
                     new Row  // addresses.address.lines.first
                     (
                        1     // addresses.address.lines.first.one
                     )
                  }
               )
            });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{Ivan;[{[{1}]}]}", ds1[0].ToString());

      }
   }
}