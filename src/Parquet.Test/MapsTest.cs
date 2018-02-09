using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class MapsTest : TestBase
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
      public void Map_in_a_struct_reads_file()
      {
         DataSet ds = ParquetReader.Read(OpenTestFile("mapinstruct.parquet"));
         Assert.Equal("{1;{1;[1=>one;2=>two;3=>three]}}", ds[0].ToString());
      }

      [Fact]
      public void Map_in_a_struct_writes_reads()
      {
         var ds = new DataSet(
            new DataField<int>("id"),
            new StructField("structure",
               new DataField<int>("id"),
               new MapField("map",
                  new DataField<int>("key"),
                  new DataField<string>("value")
            )));
         ds.Add(1, new Row(1, new Dictionary<int, string>
         {
            [1] = "one",
            [2] = "two",
            [3] = "three"
         }));
         Assert.Equal("{1;{1;[1=>one;2=>two;3=>three]}}", ds.WriteReadFirstRow());
      }
   }
}
