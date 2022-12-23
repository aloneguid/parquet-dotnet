using NetBox.IO;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using Xunit;
using NetBox.Generator;
using Parquet.Data.Rows;
using System.Linq;
using System.Threading.Tasks;

namespace Parquet.Test {
    public class ParquetReaderTest : TestBase {
        [Fact]
        public async Task Opening_null_stream_fails() {
            await Assert.ThrowsAsync<ArgumentNullException>(async () => await ParquetReader.CreateAsync((Stream)null));
        }

        [Fact]
        public async Task Opening_small_file_fails() {
            await Assert.ThrowsAsync<IOException>(async () => await ParquetReader.CreateAsync("small".ToMemoryStream()));
        }

        [Fact]
        public async Task Opening_file_without_proper_head_fails() {
            await Assert.ThrowsAsync<IOException>(async () => await ParquetReader.CreateAsync("PAR2dataPAR1".ToMemoryStream()));
        }

        [Fact]
        public async Task Opening_file_without_proper_tail_fails() {
            await Assert.ThrowsAsync<IOException>(async () => await ParquetReader.CreateAsync("PAR1dataPAR2".ToMemoryStream()));
        }

        [Fact]
        public async Task Opening_readable_but_not_seekable_stream_fails() {
            await Assert.ThrowsAsync<ArgumentException>(async () => await ParquetReader.CreateAsync(new ReadableNonSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
        }

        [Fact]
        public async Task Opening_not_readable_but_seekable_stream_fails() {
            await Assert.ThrowsAsync<ArgumentException>(async () => await ParquetReader.CreateAsync(new NonReadableSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
        }

        [Theory]
        [InlineData("map_simple.parquet")]
        [InlineData("map_simple.v2.parquet")]
        public async Task Read_simple_map(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] data = await reader.ReadEntireRowGroupAsync();

                Assert.Equal(new int?[] { 1 }, data[0].Data);
                Assert.Equal(new int[] { 1, 2, 3 }, data[1].Data);
                Assert.Equal(new string[] { "one", "two", "three" }, data[2].Data);
            }
        }

        [Theory]
        [InlineData("complex-primitives.parquet")]
        [InlineData("complex-primitives.v2.parquet")]
        public async Task Read_hardcoded_decimal(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile))) {
                decimal value = (decimal)(await reader.ReadEntireRowGroupAsync())[1].Data.GetValue(0);
                Assert.Equal((decimal)1.2, value);
            }
        }


        [Theory]
        [InlineData("multi.page.parquet")]
        [InlineData("multi.page.v2.parquet")]
        public async Task Reads_multi_page_file(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] data = await reader.ReadEntireRowGroupAsync();
                Assert.Equal(927861, data[0].Data.Length);

                int[] firstColumn = (int[])data[0].Data;
                Assert.Equal(30763, firstColumn[524286]);
                Assert.Equal(30766, firstColumn[524287]);

                //At row 524288 the data is split into another page
                //The column makes use of a dictionary to reduce the number of values and the default dictionary index value is zero (i.e. the first record value)
                Assert.NotEqual(firstColumn[0], firstColumn[524288]);

                //The value should be 30768
                Assert.Equal(30768, firstColumn[524288]);
            }
        }

        [Theory]
        [InlineData("rle_dictionary_encoded_columns.parquet")]
        [InlineData("rle_dictionary_encoded_columns.v2.parquet")]
        public async Task Reads_rle_dictionary_encoded_columns(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] data = await reader.ReadEntireRowGroupAsync();

                //If we made it this far we were able to read all the columns
                Assert.Single(data[0].Data);
                Assert.Equal(40539, ((float?[])data[0].Data)[0]);
            }
        }

        [Theory]
        [InlineData("real/nation.plain.parquet")]
        [InlineData("real/nation.plain.v2.parquet")]
        public async Task Reads_byte_arrays(string parquetFile) {
            byte[] nameValue;
            byte[] expectedValue = Encoding.UTF8.GetBytes("ALGERIA");
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] data = await reader.ReadEntireRowGroupAsync();

                byte[][] nameColumn = (byte[][])data[1].Data;
                nameValue = nameColumn[0];
                Assert.Equal<IEnumerable<byte>>(expectedValue, nameValue);

            }
            Assert.Equal<IEnumerable<byte>>(expectedValue, nameValue);
        }

        [Theory]
        [InlineData("/special/multi_data_page.parquet")]
        [InlineData("/special/multi_data_page.v2.parquet")]
        public async Task Read_multiple_data_pages(string parquetFile) {
            using(ParquetReader reader =
               await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] columns = await reader.ReadEntireRowGroupAsync();

                string[] s = (string[])columns[0].Data;
                double?[] d = (double?[])columns[1].Data;

                // check for nulls (issue #370)
                for(int i = 0; i < s.Length; i++) {
                    Assert.True(s[i] != null, "found null in s at " + i);
                    Assert.True(d[i] != null, "found null in d at " + i);
                }

                // run aggregations checking row alignment (issue #371)
                var seq = s.Zip(d.Cast<double>(), (w, v) => new { w, v })
                   .Where(p => p.w == "favorable")
                   .ToList();

                // double matching is fuzzy, but matching strings is enough for this test
                // ground truth was computed using Spark
                Assert.Equal(26706.6185312147, seq.Sum(p => p.v), 5);
                Assert.Equal(0.808287234987281, seq.Average(p => p.v), 5);
                Assert.Equal(0.71523915461624, seq.Min(p => p.v), 5);
                Assert.Equal(0.867111980015206, seq.Max(p => p.v), 5);
            }
        }

        [Theory]
        [InlineData("/special/multi_page_dictionary_with_nulls.parquet")]
        [InlineData("/special/multi_page_dictionary_with_nulls.v2.parquet")]
        public async Task Read_multi_page_dictionary_with_nulls(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile))) {
                DataColumn[] columns = await reader.ReadEntireRowGroupAsync();
                ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);

                // reading columns
                string[] data = (string[])columns[0].Data;

                // ground truth from spark
                // check page boundary (first page contains 107432 rows)
                Assert.Equal("xc3w4eudww", data[107432]);
                Assert.Equal("bpywp4wtwk", data[107433]);
                Assert.Equal("z6x8652rle", data[107434]);

                // check near the end of the file
                Assert.Null(data[310028]);
                Assert.Equal("wok86kie6c", data[310029]);
                Assert.Equal("le9i7kbbib", data[310030]);
            }
        }

        [Theory]
        [InlineData("/special/multi_page_bit_packed_near_page_border.parquet")]
        [InlineData("/special/multi_page_bit_packed_near_page_border.v2.parquet")]
        public async Task Read_bit_packed_at_page_boundary(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile))) {
                DataColumn[] columns = await reader.ReadEntireRowGroupAsync();
                string[] data = (string[])columns[0].Data;

                // ground truth from spark
                Assert.Equal(30855, data.Count(string.IsNullOrEmpty));
                // check page boundary
                Assert.Equal("collateral_natixis_fr_vol5010", data[60355]);
                Assert.Equal("BRAZ82595832_vol16239", data[60356]);
            }
        }

        [Theory]
        [InlineData("/mixed-dictionary-plain.parquet")]
        [InlineData("/mixed-dictionary-plain.v2.parquet")]
        public async Task ReadLargeTimestampData(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] columns = await reader.ReadEntireRowGroupAsync();

                DateTimeOffset?[] col0 = (DateTimeOffset?[])columns[0].Data;
                Assert.Equal(440773, col0.Length);

                long ticks = col0[0].Value.Ticks;
                for(int i = 1; i < 132000; i++) {
                    long now = col0[i].Value.Ticks;
                    Assert.NotEqual(ticks, now);
                }
            }
        }

        [Theory]
        [InlineData("map_simple.parquet")]
        [InlineData("map_simple.v2.parquet")]
        public async Task ParquetReader_OpenFromFile_Close_Stream(string parquetFile) {
            // copy a file to a temp location
            string tempFile = Path.GetTempFileName();
            using(Stream fr = OpenTestFile(parquetFile))
            using(FileStream fw = System.IO.File.OpenWrite(tempFile)) {
                fr.CopyTo(fw);
            }

            // open the copy
            using(ParquetReader reader = await ParquetReader.CreateAsync(tempFile)) {
                // do nothing
            }

            // now try to delete this temp file. If the stream is properly closed, this should succeed
            System.IO.File.Delete(tempFile);
        }

        [Theory]
        [InlineData("emptycolumn.parquet")]
        [InlineData("emptycolumn.v2.parquet")]
        public async Task ParquetReader_EmptyColumn(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] columns = await reader.ReadEntireRowGroupAsync();
                int?[] col0 = (int?[])columns[0].Data;
                Assert.Equal(10, col0.Length);
                foreach(int? value in col0) {
                    Assert.Null(value);
                }
            }
        }

        class ReadableNonSeekableStream : DelegatedStream {
            public ReadableNonSeekableStream(Stream master) : base(master) {
            }

            public override bool CanSeek => false;

            public override bool CanRead => true;
        }

        class NonReadableSeekableStream : DelegatedStream {
            public NonReadableSeekableStream(Stream master) : base(master) {
            }

            public override bool CanSeek => true;

            public override bool CanRead => false;
        }

        class ReadableAndSeekableStream : DelegatedStream {
            public ReadableAndSeekableStream(Stream master) : base(master) {
            }

            public override bool CanSeek => true;

            public override bool CanRead => true;

        }
    }
}