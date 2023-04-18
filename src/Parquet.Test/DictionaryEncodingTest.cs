using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class DictionaryEncodingTest : TestBase {

        [Fact]
        public async Task DictionaryEncodingTest2() {
            string?[] data = new string?[]
            {
            "xxx",
            "xxx",
            "xxx",
            "xxx",
            "xxx",
            null,
            "yyy",
            "yyy",
            "yyy",
            string.Empty,
            "yyy",
            "yyy",
            null,
            null,
            "zzz",
            "zzz",
            "zzz",
            "zzz",
            };

            var dataField = new DataField<string>("string");
            var parquetSchema = new ParquetSchema(dataField);

            using var stream = new MemoryStream();

            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, stream, formatOptions: new ParquetOptions() { UseDictionaryEncoding = true })) {
                using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
                await groupWriter.WriteColumnAsync(new DataColumn(dataField, data));
            }

            using ParquetReader parquetReader = await ParquetReader.CreateAsync(stream);
            using ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0);
            Data.DataColumn dataColumn = await groupReader.ReadColumnAsync(dataField);

            Assert.Equal(data, dataColumn.Data);
        }

        [Fact]
        public async Task ReadStringDictionaryGeneratedBySpark() {
            using Stream fs = OpenTestFile("string_dictionary_by_spark.parquet");
            using ParquetReader reader = await ParquetReader.CreateAsync(fs);

            DataColumn[] cols = await reader.ReadEntireRowGroupAsync(0);
            Assert.Single(cols);
            DataColumn c0 = cols[0];

            Assert.Equal(400, c0.NumValues);
            Assert.Equal(Enumerable.Repeat("one", 100).ToArray(), c0.AsSpan<string>(0, 100).ToArray());
            Assert.Equal(Enumerable.Repeat("two", 100).ToArray(), c0.AsSpan<string>(100, 100).ToArray());
            Assert.Equal(Enumerable.Repeat((string?)null, 100).ToArray(), c0.AsSpan<string>(200, 100).ToArray());
            Assert.Equal(Enumerable.Repeat("three", 100).ToArray(), c0.AsSpan<string>(300, 100).ToArray());
        }
    }
}