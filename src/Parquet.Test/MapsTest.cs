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
            new SchemaElement<int>("id"),
            new SchemaElement<IDictionary<int, string>>("names"));

         ds.Add(1, new Dictionary<int, string>
         {
            [1] = "one",
            [2] = "two"
         });

         //ParquetWriter.WriteFile(ds, "c:\\tmp\\map.parquet");

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         Assert.Equal("{1;[1=>one;2=>two]}", ds1[0].ToString());
      }
   }
}
