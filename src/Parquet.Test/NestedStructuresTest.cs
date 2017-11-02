using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class NestedStructuresTest
   {
      [Fact]
      public void Simple_nested_structure_write_read()
      {
         var ds = new DataSet(
            new SchemaElement<string>("name"),
            new SchemaElement<Row>("address",
               new SchemaElement<string>("line1"),
               new SchemaElement<string>("postcode")
            ));

         ds.Add("Ivan", new Row("Woods", "postcode"));
         Assert.Equal(1, ds.RowCount);
         Assert.Equal(2, ds.ColumnCount);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);
      }

      [Fact]
      public void Simple_repeated_field_write_read()
      {
         var ds = new DataSet(
            new SchemaElement<int>("id"),
            new SchemaElement<IEnumerable<string>>("items"));

         ds.Add(1, new[] { "one", "two" });

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal(1, ds1[0][0]);
         Assert.Equal(new[] { "one", "two" }, ds1[0][1]);
      }
   }
}