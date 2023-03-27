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
            var field = new DataField<IEnumerable<int>>("items");
            var column = new DataColumn(
               field,
               new int[] { 1, 2, 3, 4, 5 },
               new int[] { 0, 1, 1, 0, 1 });

            // act
            DataColumn? rc = await WriteReadSingleColumn(field, column);

            // assert
            Assert.Equal(new int[] { 1, 2, 3, 4, 5 }, rc!.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0, 1 }, rc!.RepetitionLevels);
        }

        [Fact]
        public async Task Repeated_field_with_nulls_write_read() {
            // Simulated parquet file:
            // | [1, 2] |
            // | []     |
            // | [4, 5] |

            // Test will pass only if DataField is set nullable
            var field = new DataField<IEnumerable<int>>("items") { IsNullable = true };
            var column = new DataColumn(
                field,
                new int?[] { 1, 2, null, 4, 5 },
                new int[] { 0, 1, 0, 0, 1 });

            // act
            DataColumn? rc = await WriteReadSingleColumn(field, column);

            // assert
            Assert.Equal(new int?[] { 1, 2, null, 4, 5 }, rc!.Data);
            Assert.Equal(new int[] { 0, 1, 0, 0, 1 }, rc!.RepetitionLevels);
        }
    }
}
