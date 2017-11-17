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
            new SchemaElement<IDictionary<int, string>>("names"),
            new SchemaElement<int>("id"));

         ds.Add(new Dictionary<int, string>
         {
            [1] = "one",
            [2] = "two",
            [3] = "three"
         }, 1);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{[1=>one;2=>two;3=>three];1}", ds1[0].ToString());
      }
   }
}
