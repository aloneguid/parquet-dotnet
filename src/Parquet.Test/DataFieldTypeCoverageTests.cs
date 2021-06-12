using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;
using Parquet.File.Values.Primitives;
using NetBox.Extensions;
using System.Linq;

namespace Parquet.Test
{
   public class DataFieldTypeCoverageTests : TestBase
   {
      /// <summary>
      /// Checking if all <see cref="DataField"/> objects contstructed
      /// via the <see cref="DataField.DataField(string, DataType, bool, bool)"/>
      /// constructor contain non-null values for <see cref="DataField.ClrType"/>
      /// and <see cref="DataField.ClrNullableIfHasNullsType"/>.
      /// </summary>
      /// <param name="type"></param>
      [Theory]
      [InlineData(DataType.Boolean)]
      [InlineData(DataType.Byte)]
      [InlineData(DataType.ByteArray)]
      [InlineData(DataType.DateTimeOffset)]
      [InlineData(DataType.Decimal)]
      [InlineData(DataType.Double)]
      [InlineData(DataType.Float)]
      [InlineData(DataType.Int16)]
      [InlineData(DataType.Int32)]
      [InlineData(DataType.Int64)]
      [InlineData(DataType.Int96)]
      [InlineData(DataType.Interval)]
      [InlineData(DataType.Short)]
      [InlineData(DataType.SignedByte)]
      [InlineData(DataType.String)]
      [InlineData(DataType.UnsignedByte)]
      [InlineData(DataType.UnsignedInt16)]
      [InlineData(DataType.UnsignedShort)]
      public void CheckingForClrType(DataType type)
      {
         DataField input = new DataField(type.ToString(), type);

         Assert.NotNull(input.ClrType);
         Assert.NotNull(input.ClrNullableIfHasNullsType);
      }
   }
}
