using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class StructureTest : TestBase
   {
      [Fact]
      public void Simple_structure_write_read()
      {
         var schema = new Schema(
            new DataField<string>("name"),
            new StructField("address",
               new DataField<string>("line1"),
               new DataField<string>("postcode")
            ));

         var ms = new MemoryStream();
         ms.WriteSingleRowGroupParquetFile(schema, 1,
            new DataColumn(new DataField<string>("name"), new[] { "Ivan" }),
            new DataColumn(new DataField<string>("line1"), new[] { "woods" }),
            new DataColumn(new DataField<string>("postcode"), new[] { "postcode" }));
         ms.Position = 0;

         ms.ReadSingleRowGroupFile(out Schema readSchema, out DataColumn[] readColumns);

         Assert.Equal("Ivan", readColumns[0].Data.GetValue(0));
         Assert.Equal("woods", readColumns[1].Data.GetValue(0));
         Assert.Equal("postcode", readColumns[2].Data.GetValue(0));
      }


      /*[Fact]
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

      }*/
   }
}