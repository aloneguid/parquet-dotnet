using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class StructureTest : TestBase
   {
      [Fact]
      public async Task Simple_structure_write_readAsync()
      {
         var schema = new Schema(
            new DataField<string>("name"),
            new StructField("address",
               new DataField<string>("line1"),
               new DataField<string>("postcode")
            ));

         var ms = new MemoryStream();
         await ms.WriteSingleRowGroupParquetFileAsync(schema,
            new DataColumn(new DataField<string>("name"), new[] { "Ivan" }),
            new DataColumn(new DataField<string>("line1"), new[] { "woods" }),
            new DataColumn(new DataField<string>("postcode"), new[] { "postcode" })).ConfigureAwait(false);
         ms.Position = 0;

         (Schema Schema, DataColumn[] readColumns) = await ms.ReadSingleRowGroupParquetFileAsync().ConfigureAwait(false);

         Assert.Equal("Ivan", readColumns[0].Data.GetValue(0));
         Assert.Equal("woods", readColumns[1].Data.GetValue(0));
         Assert.Equal("postcode", readColumns[2].Data.GetValue(0));
      }
   }
}