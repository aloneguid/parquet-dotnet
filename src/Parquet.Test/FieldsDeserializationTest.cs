using System.Linq;
using System.Threading.Tasks;
using Parquet.Rows;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class FieldsDeserializationTest {
        [Fact]
        public async void DeserializeDecimalsAsync() {
            var table = await ReadTableAsync("types/decimal_precision");
            var fields = new[] {
                new DecimalDataField("small_decimal", 29, 6, isNullable: true),
                new DecimalDataField("big_decimal", 38, 18, isNullable: true)
            };
            for (int i = 0; i < fields.Length; i++) {
                fields[i].SchemaElement = table.Schema.Fields[i].SchemaElement;
            }
            var expectedSchema = new ParquetSchema(fields);

            Assert.All(table.Schema.Fields, field => Assert.IsType<DecimalDataField>(field));
            Assert.All(table.Schema.Fields, (field, i) => Assert.Equivalent(expectedSchema.Fields[i], field));
        }

        [Fact]
        public async void DeserializeAllTypesAsync() {
            var table = await ReadTableAsync("types/alltypes.plain");

            Assert.All(table.Schema.Fields.SkipLast(1), field => Assert.IsType<DataField>(field));
            Assert.IsType<DateTimeDataField>(table.Schema.Fields.Last());
        }

        private static async Task<Table> ReadTableAsync(string fileName) {
            string filePath = $"./data/{fileName}.parquet";
            using var reader = await ParquetReader.CreateAsync(filePath);
            var table = await reader.ReadAsTableAsync();
            return table;
        }
    }
}