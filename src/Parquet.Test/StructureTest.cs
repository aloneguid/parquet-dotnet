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
         ms.WriteSingleRowGroupParquetFile(schema,
            new DataColumn(new DataField<string>("name"), new[] { "Ivan" }),
            new DataColumn(new DataField<string>("line1"), new[] { "woods" }),
            new DataColumn(new DataField<string>("postcode"), new[] { "postcode" }));
         ms.Position = 0;

         ms.ReadSingleRowGroupParquetFile(out Schema readSchema, out DataColumn[] readColumns);

         Assert.Equal("Ivan", readColumns[0].Data.GetValue(0));
         Assert.Equal("woods", readColumns[1].Data.GetValue(0));
         Assert.Equal("postcode", readColumns[2].Data.GetValue(0));
      }
   }
}