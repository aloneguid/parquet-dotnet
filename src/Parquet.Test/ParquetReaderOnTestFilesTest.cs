using Parquet.Data;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Parquet.Test {
    /// <summary>
    /// Tests a set of predefined test files that they read back correct
    /// </summary>
    public class ParquetReaderOnTestFilesTest : TestBase {
        private byte[] vals = new byte[18]
        {
         0x00,
         0x00,
         0x27,
         0x79,
         0x7f,
         0x26,
         0xd6,
         0x71,
         0xc8,
         0x00,
         0x00,
         0x4e,
         0xf2,
         0xfe,
         0x4d,
         0xac,
         0xe3,
         0x8f
        };

        [Fact]
        public async Task FixedLenByteArray_dictionary() {
            using(Stream s = OpenTestFile("fixedlenbytearray.parquet")) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();
                }
            }
        }

        [Fact]
        public async Task Datetypes_all() {
            DateTimeOffset offset, offset2;
            using(Stream s = OpenTestFile("dates.parquet")) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();

                    offset = (DateTimeOffset)(columns[1].Data.GetValue(0));
                    offset2 = (DateTimeOffset)(columns[1].Data.GetValue(1));
                }
            }
            Assert.Equal(new DateTime(2017, 1, 1), offset.Date);
            Assert.Equal(new DateTime(2017, 2, 1), offset2.Date);
        }

        [Fact]
        public async Task DateTime_FromOtherSystem() {
            DateTimeOffset offset;
            using(Stream s = OpenTestFile("datetime_other_system.parquet")) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();

                    DataColumn as_at_date_col = columns.FirstOrDefault(x => x.Field.Name == "as_at_date_");
                    Assert.NotNull(as_at_date_col);

                    offset = (DateTimeOffset)(as_at_date_col.Data.GetValue(0));
                    Assert.Equal(new DateTime(2018, 12, 14, 0, 0, 0), offset.Date);
                }
            }
        }
        [Fact]
        public async Task OptionalValues_WithoutStatistics() {
            using(Stream s = OpenTestFile("test-optionals-without-stats.parquet")) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();
                    DataColumn id_col = columns.FirstOrDefault(x => x.Field.Name == "id");
                    DataColumn value_col = columns.FirstOrDefault(x => x.Field.Name == "value");
                    Assert.NotNull(id_col);
                    Assert.NotNull(value_col);

                    int index = Enumerable.Range(0, id_col.Data.Length).First(i => (long)id_col.Data.GetValue(i) == 20908539289);

                    Assert.Equal(0, value_col.Data.GetValue(index));
                }
            }
        }

        /*[Fact]
        public async Task Issue164() {
            using(Stream s = OpenTestFile("issue-164.parquet")) {
                using(ParquetReader r = await ParquetReader.CreateAsync(s)) {
                    DataColumn[] columns = await r.ReadEntireRowGroupAsync();
                    DataColumn id_col = columns[0];
                    DataColumn cls_value_8 = columns[9];
                    int index = Enumerable.Range(0, id_col.Data.Length).First(i => (int)id_col.Data.GetValue(i) == 256779);
                    Assert.Equal("MOSTRU\u00C1RIO-000", cls_value_8.Data.GetValue(index));

                }
            }
        }*/

    }
}