using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types {
    public class RepeatableFieldsTest : TestBase {

        [Fact]
        public async Task Simple_repeated_field_write_read() {
            // arrange 
            var schema = new ParquetSchema(new DataField<IEnumerable<int>>("items"));
            var column = new DataColumn(
               schema.GetDataFields()[0],
               new int[] { 1, 2, 3, 4, 5 },
               new int[] { 0, 1, 1, 0, 1 });

            // act
            DataColumn? rc = await WriteReadSingleColumn(column);

            // assert
            Assert.Equal(new int[] { 1, 2, 3, 4, 5 }, rc!.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0, 1 }, rc!.RepetitionLevels);
        }

        [Fact]
        public async Task Nullable_repeated_field_write_read() {
            // arrange 
            var schema = new ParquetSchema(new DataField<IEnumerable<int?>>("items"));
            var column = new DataColumn(
               schema.GetDataFields()[0],
               new int?[] { 1, 2, null, 4, 5 },
               new int[] { 0, 1, 1, 0, 1 });

            // act
            DataColumn? rc = await WriteReadSingleColumn(column);

            // assert
            Assert.Equal(new int?[] { 1, 2, null, 4, 5 }, rc!.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0, 1 }, rc!.RepetitionLevels);
        }

        [Fact]
        public async Task Nullable1_repeated_field_write_read() {
            // arrange 
            var schema = new ParquetSchema(new DataField<IEnumerable<int>>("items") { IsNullable = true });
            var column = new DataColumn(
               schema.GetDataFields()[0],
               new int?[] { 1, 2, 3, 4, 5 },
               new int[] { 0, 1, 1, 0, 1 });

            // act
            DataColumn? rc = await WriteReadSingleColumn(column);

            // assert
            Assert.Equal(new int[] { 1, 2, 3, 4, 5 }, rc!.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0, 1 }, rc!.RepetitionLevels);
        }
    }
}
