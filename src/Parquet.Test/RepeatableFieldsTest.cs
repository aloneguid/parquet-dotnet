using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class RepeatableFieldsTest : TestBase
   {
      [Fact]
      public async Task Simple_repeated_field_write_readAsync()
      {
         // arrange 
         var field = new DataField<IEnumerable<int>>("items");
         var column = new DataColumn(
            field,
            new int[] { 1, 2, 3, 4 },
            new int[] { 0, 1, 0, 1 });

         // act
         DataColumn rc = await WriteReadSingleColumnAsync(field, column).ConfigureAwait(false);

         // assert
         Assert.Equal(new int[] { 1, 2, 3, 4 }, rc.Data);
         Assert.Equal(new int[] { 0, 1, 0, 1 }, rc.RepetitionLevels);

         // https://github.com/aloneguid/parquet-dotnet/blob/final-v2/src/Parquet/File/RepetitionPack.cs

         // tests: https://github.com/aloneguid/parquet-dotnet/blob/final-v2/src/Parquet.Test/RepetitionsTest.cs

      }
   }
}
