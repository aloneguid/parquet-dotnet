using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class MapsTest
   {
      [Fact]
      public void Simple_first_level_map_int_to_string()
      {
         var ds = new DataSet(
            new MapField("names", new DataField("key", DataType.Int32), new DataField("value", DataType.String)),
            new DataField<int>("id"));

         ds.Add(new Dictionary<int, string>
         {
            [1] = "one",
            [2] = "two",
            [3] = "three"
         }, 1);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{[1=>one;2=>two;3=>three];1}", ds1[0].ToString());
      }

      [Fact]
      public void Map_in_a_struct()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new StructField("random",
               new DataField<string>("r1"),
               new MapField("keys",
                  new DataField<int>("key"),
                  new DataField<string>("value")
            )));
         ds.Add(1, new Row("r1", new Dictionary<int, string>
         {
            [1] = "one",
            [2] = "two"
         }));
         Assert.Equal("", ds.WriteReadFirstRow());
      }
   }
}
