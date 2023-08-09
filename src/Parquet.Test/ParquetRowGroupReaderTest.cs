using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class ParquetRowGroupReaderTest : TestBase {

        [Theory]
        [InlineData("multi.page.parquet")]
        [InlineData("multi.page.v2.parquet")]
        public async Task GetColumnStatistics_ShouldNotBeEmpty(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                for(int gidx = 0; gidx < reader.RowGroupCount; gidx++) {
                    using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0)) {

                        foreach(DataField df in reader.Schema.DataFields) {
                            DataColumnStatistics? stats = rowGroupReader.GetStatistics(df);

                            Assert.NotNull(stats);
                        }
                    }
                }
            }

        }

        [Theory]
        [InlineData("multi.page.parquet")]
        [InlineData("multi.page.v2.parquet")]
        public async Task GetColumnReader_MustFailOnInvalidField(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0)) {

                    Assert.Throws<ArgumentNullException>(() => rowGroupReader.GetStatistics(null!));
                    DataField nonExistingField = new DataField("non_existing_field7862425", typeof(int));
                    Assert.Throws<ParquetException>(() => rowGroupReader.GetStatistics(nonExistingField));
                }
            }
        }

    }

}




