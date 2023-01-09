using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class DataFieldTypeCoverageTests : TestBase {
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
        [InlineData(DataType.SignedByte)]
        [InlineData(DataType.String)]
        [InlineData(DataType.UnsignedInt16)]
        public void CheckingForClrType(DataType type) {
            DataField input = new DataField(type.ToString(), type);

            Assert.NotNull(input.ClrType);
            Assert.NotNull(input.ClrNullableIfHasNullsType);
        }
    }
}