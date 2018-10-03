using System.Collections.Generic;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class RepeatableFieldsTest : TestBase
   {
      [Fact]
      public void Simple_repeated_field_write_read()
      {
         // arrange 
         var field = new DataField<IEnumerable<int>>("items");
         var column = new DataColumn(
            field,
            new int[] { 1, 2, 3, 4 },
            new int[] { 0, 1, 0, 1 });

         // act
         DataColumn rc = WriteReadSingleColumn(field, column);

         // assert
         Assert.Equal(new int[] { 1, 2, 3, 4 }, rc.Data);
         Assert.Equal(new int[] { 0, 1, 0, 1 }, rc.RepetitionLevels);

         // https://github.com/elastacloud/parquet-dotnet/blob/final-v2/src/Parquet/File/RepetitionPack.cs

         // tests: https://github.com/elastacloud/parquet-dotnet/blob/final-v2/src/Parquet.Test/RepetitionsTest.cs

      }
   }
}
