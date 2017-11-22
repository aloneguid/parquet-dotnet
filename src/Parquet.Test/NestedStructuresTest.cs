using System.Collections.Generic;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class NestedStructuresTest
   {
      [Fact]
      public void Simple_structure_write_read()
      {
         var ds = new DataSet(
            new Field<string>("name"),
            new StructField("address",
               new Field<string>("line1"),
               new Field<string>("postcode")
            ));

         ds.Add("Ivan", new Row("Woods", "postcode"));
         Assert.Equal(1, ds.RowCount);
         Assert.Equal(2, ds.ColumnCount);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{Ivan;{Woods;postcode}}", ds1[0].ToString());
      }

      [Fact]
      public void Simple_repeated_field_write_read()
      {
         var ds = new DataSet(
            new Field<int>("id"),
            new Field<IEnumerable<string>>("items"));

         ds.Add(1, new[] { "one", "two" });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal(1, ds1[0][0]);
         Assert.Equal(new[] { "one", "two" }, ds1[0][1]);
      }

      [Fact]
      public void List_of_structures_writes_reads()
      {
         var ds = new DataSet(
            new Field<int>("id"),
            new ListField("cities",
            new StructField("element",
               new Field<string>("name"),
               new Field<string>("country"))));

         ds.Add(1, new[] { new Row("London", "UK"), new Row("New York", "US") });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{1;[{London;UK};{New York;US}]}", ds1[0].ToString());
      }

      [Fact]
      public void Structure_nested_into_structure_write_read()
      {
         var ds = new DataSet(
            new Field<string>("name"),
            new StructField("address",
               new Field<string>("name"),
               new StructField("lines",
                  new Field<string>("line1"),
                  new Field<string>("line2"))));

         ds.Add("Ivan", new Row("Primary", new Row("line1", "line2")));

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{Ivan;{Primary;{line1;line2}}}", ds1[0].ToString());
      }

      [Fact]
      public void Structure_with_repeated_field_writes_reads()
      {
         var ds = new DataSet(
            new Field<string>("name"),
            new StructField("address",
               new Field<string>("name"),
               new Field<IEnumerable<string>>("lines")));

         ds.Add("Ivan", new Row("Primary", new[] { "line1", "line2" }));

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{Ivan;{Primary;[line1;line2]}}", ds1[0].ToString());
      }
   }
}