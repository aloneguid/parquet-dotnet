using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types {
    public class EndToEndDateTimeKindTest : TestBase {
        private static DateTime BaseDate => DateTime.Parse("2020-06-10T11:12:13", CultureInfo.InvariantCulture);
        private static DateTime UtcDate => DateTime.SpecifyKind(BaseDate, DateTimeKind.Utc);
        private static DateTime UnknownDate => DateTime.SpecifyKind(BaseDate, DateTimeKind.Unspecified);
        private static DateTime LocalDate => DateTime.SpecifyKind(BaseDate, DateTimeKind.Local);

        private static DateTime ExpectedUnspecified => DateTime.SpecifyKind(BaseDate, DateTimeKind.Unspecified);
        private static DateTime ExpectedLocal => DateTime.SpecifyKind(LocalDate.ToUniversalTime(), DateTimeKind.Unspecified);

        [Fact]
        public async Task Int96_dates_round_trip_as_unknown() {
            var schema = new ParquetSchema(new DataField<DateTime>("datetime"));
            DataField field = schema.DataFields[0];
            var column = new DataColumn(field, new[] { UtcDate, UnknownDate, LocalDate });
            DataColumn? actualColumn = await WriteReadSingleColumn(column);
            Assert.NotNull(actualColumn);

            DateTime[] actualValues = Assert.IsType<DateTime[]>(actualColumn.Data);
            Assert.Equal(3, actualValues.Length);
            Assert.Equal(ExpectedUnspecified, actualValues[0]); // utc will be converted to unspecified in the round trip
            Assert.Equal(ExpectedUnspecified, actualValues[1]);
            Assert.Equal(ExpectedLocal, actualValues[2]);
            Assert.True(actualValues.All(d => d.Kind == DateTimeKind.Unspecified));
        }
    }
}
