using NetBox.IO;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;
using NetBox.Generator;
using System.Linq;
using System.Threading.Tasks;
using Path = System.IO.Path;
using System.Threading;
using Parquet.Schema;

namespace Parquet.Test {
    public class ParquetReaderTest : TestBase {

        [Fact]
        public async Task Opening_small_file_fails() {
            await Assert.ThrowsAsync<IOException>(async () => await ParquetReader.CreateAsync("small".ToMemoryStream()!));
        }

        [Fact]
        public async Task Opening_file_without_proper_head_fails() {
            await Assert.ThrowsAsync<IOException>(async () => await ParquetReader.CreateAsync("PAR2dataPAR1".ToMemoryStream()!));
        }

        [Fact]
        public async Task Opening_file_without_proper_tail_fails() {
            await Assert.ThrowsAsync<IOException>(async () => await ParquetReader.CreateAsync("PAR1dataPAR2".ToMemoryStream()!));
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

        [Fact(Skip = "todo: for some reason .Read (sync) is still called")]
        public async Task Reading_schema_uses_async_only_methods() {
            using Stream tf = OpenTestFile("map_simple.parquet");
            using Stream ao = new AsyncOnlyStream(tf, 3);
            using ParquetReader reader = await ParquetReader.CreateAsync(ao);

            Assert.NotNull(reader.Schema);
        }

        [Theory]
        [InlineData("complex-primitives.parquet")]
        [InlineData("complex-primitives.v2.parquet")]
        public async Task Read_hardcoded_decimal(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile))) {
                decimal value = (decimal)(await reader.ReadEntireRowGroupAsync())[1].Data.GetValue(0)!;
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
        // v2 has a mix of pages: dictionary, data (dictionary indexes), data (plain values)
        // therefore the reader must merge dictionary indexes into data asap to avoid data pages with different encodings
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

                DateTime?[] col0 = (DateTime?[])columns[0].Data;
                Assert.Equal(440773, col0.Length);

                long ticks = col0[0]!.Value.Ticks;
                for(int i = 1; i < 132000; i++) {
                    long now = col0[i]!.Value.Ticks;
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

        [Theory]
        [InlineData("timestamp_micros.parquet")]
        [InlineData("timestamp_micros.v2.parquet")]
        public async Task ParquetReader_TimestampMicrosColumn(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                DataColumn[] columns = await reader.ReadEntireRowGroupAsync();
                var col0 = (DateTime?[])columns[0].Data;
                Assert.Equal(3, col0.Length);
                Assert.Equal(new DateTime(2022,12,23,11,43,49).AddTicks(10 * 10), col0[0]);
                Assert.Equal(new DateTime(2021,12,23,12,44,50).AddTicks(11 * 10), col0[1]);
                Assert.Equal(new DateTime(2020,12,23,13,45,51).AddTicks(12 * 10), col0[2]);
            }
        }

        [Fact]
        public async Task Metadata_file() {
            using ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile("geoparquet/example.parquet"));
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            Assert.True(reader.CustomMetadata.ContainsKey("geo"));
            Assert.True(reader.CustomMetadata.ContainsKey("ARROW:schema"));
        }

        [Theory]
        [InlineData("multi.page.parquet")]
        [InlineData("multi.page.v2.parquet")]
        public async Task RowGroups_EnumeratesRowGroups(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {

                Assert.Single(reader.RowGroups);
                IParquetRowGroupReader rowGroup = reader.RowGroups.Single();
                Assert.Equal(927861, rowGroup.RowCount);
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

        class AsyncOnlyStream : Stream {
            private readonly Stream _baseStream;
            private readonly int _maxSyncReads;
            private int _syncReads = 0;

            public AsyncOnlyStream(Stream baseStream, int maxSyncReads) {
                _baseStream = baseStream;
                _maxSyncReads = maxSyncReads;
            }

            public override bool CanRead => _baseStream.CanRead;

            public override bool CanSeek => _baseStream.CanSeek;

            public override bool CanWrite => _baseStream.CanWrite;

            public override long Length => _baseStream.Length;

            public override long Position { get => _baseStream.Position; set => _baseStream.Position = value; }

            public override void Flush() => _baseStream.Flush();

            public override int Read(byte[] buffer, int offset, int count) {
                _syncReads++;
                if(_syncReads > _maxSyncReads) {
                    throw new IOException($"limit of {_maxSyncReads} reached");
                }
                return _baseStream.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin) => _baseStream.Seek(offset, origin);

            public override void SetLength(long value) => _baseStream.SetLength(value);

            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }
    }
}