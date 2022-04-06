using System;
using Parquet.Data.Rows;
using Parquet.Data;
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

      [Fact]
      public void Validate_Scale_Zero_Should_Be_Allowed()
      {
         const int precision = 1;
         const int scale = 0;
         var field = new DecimalDataField("field-name", precision, scale);
         Assert.Equal(field.Scale, scale);
      }
      
      [Fact]
      public void Validate_Negative_Scale_Should_Throws_Exception()
      {
         const int precision = 1;
         const int scale = -1;
         var ex =  Assert.Throws<ArgumentException>(() => new DecimalDataField("field-name", precision, scale));
         Assert.Equal(ex.Message, "scale must be zero or a positive integer (Parameter 'scale')");
      }
      
      [Fact]
      public void Validate_Precision_Zero_Should_Throws_Exception()
      {
         const int precision = 0;
         const int scale = 1;
         var ex =  Assert.Throws<ArgumentException>(() => new DecimalDataField("field-name", precision, scale));
         Assert.Equal(ex.Message, "precision is required and must be a non-zero positive integer (Parameter 'precision')");
      }
      
      [Fact]
      public void Validate_Scale_Bigger_Then_Precision_Throws_Exception()
      {
         const int precision = 3;
         const int scale = 4;
         var ex =  Assert.Throws<ArgumentException>(() => new DecimalDataField("field-name", precision, scale));
         Assert.Equal(ex.Message, "scale must be less than the precision (Parameter 'scale')");
      }
   }
}