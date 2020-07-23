using System.Threading.Tasks;
using Parquet.Data.Rows;
using Xunit;

namespace Parquet.Test
{
   public class DecimalTypeTest : TestBase
   {
      [Fact]
      public async Task Read_File_As_Table_With_Decimal_Column_Should_Read_FileAsync()
      {
         const int decimalColumnIndex = 4;
         Table table = await ReadTestFileAsTableAsync("test-types-with-decimal.parquet").ConfigureAwait(false);

         Assert.Equal(1234.56m, table[0].Get<decimal>(decimalColumnIndex));
      }
   }
}