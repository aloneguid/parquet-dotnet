using Parquet.Data.Rows;
using Xunit;

namespace Parquet.Test
{
   public class DecimalTypeTest : TestBase
   {
      [Fact]
      public void Read_File_As_Table_With_Decimal_Column_Should_Read_File()
      {
         const int decimalColumnIndex = 4;
         Table table = ReadTestFileAsTable("test-types-with-decimal.parquet");

         Assert.Equal(1234.56m, table[0].Get<decimal>(decimalColumnIndex));
      }
   }
}