using System;
using Parquet.Data;
using System.IO;
using Xunit;
using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Schema;
using System.Linq;

namespace Parquet.Test {
    public class ParquetWriterTest : TestBase {
        [Fact]
        public async Task Cannot_write_columns_in_wrong_order() {
            var schema = new ParquetSchema(new DataField<int>("id"), new DataField<int>("id2"));

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, new MemoryStream())) {

                using(ParquetRowGroupWriter gw = writer.CreateRowGroup()) {
                    await Assert.ThrowsAsync<ArgumentException>(async () => {
                        await gw.WriteColumnAsync(new DataColumn((DataField)schema[1], new int[] { 1 }));
                    });
                }
            }
        }

        [Fact]
        public async Task Write_in_small_RowGroups() {
            //write a single file having 3 row groups
            var id = new DataField<int>("id");
            var ms = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 1 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 2 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 3 }));
                }

            }

            //read the file back and validate
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(3, reader.RowGroupCount);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(1, rg.RowCount);
                    DataColumn dc = await rg.ReadColumnAsync(id);
                    Assert.Equal(new int[] { 1 }, dc.Data);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(1, rg.RowCount);
                    DataColumn dc = await rg.ReadColumnAsync(id);
                    Assert.Equal(new int[] { 2 }, dc.Data);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(2)) {
                    Assert.Equal(1, rg.RowCount);
                    DataColumn dc = await rg.ReadColumnAsync(id);
                    Assert.Equal(new int[] { 3 }, dc.Data);
                }
            }
        }

#if NET7_0_OR_GREATER
        [Fact]
        public async Task Write_in_small_RowGroups_write_only_stream() {
            //write to a write-only stream that does not implement the Position property
            var id = new DataField<int>("id");
            var pipe = new System.IO.Pipelines.Pipe();
            Stream ws = pipe.Writer.AsStream();

            //parallel thread to read the file back and validate
            var reader = Task.Run(async () => {
                //ParquetReader, unlike the writer, does need to be seekable
                var ms = new MemoryStream();
                await pipe.Reader.AsStream().CopyToAsync(ms);
                using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                    Assert.Equal(3, reader.RowGroupCount);

                    using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                        Assert.Equal(1, rg.RowCount);
                        DataColumn dc = await rg.ReadColumnAsync(id);
                        Assert.Equal(new int[] { 1 }, dc.Data);
                    }

                    using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                        Assert.Equal(1, rg.RowCount);
                        DataColumn dc = await rg.ReadColumnAsync(id);
                        Assert.Equal(new int[] { 2 }, dc.Data);
                    }

                    using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(2)) {
                        Assert.Equal(1, rg.RowCount);
                        DataColumn dc = await rg.ReadColumnAsync(id);
                        Assert.Equal(new int[] { 3 }, dc.Data);
                    }
                }
            });

            // actual writing now that the reader is set up
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ws)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 1 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 2 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 3 }));
                }
            }
            ws.Dispose();

            //run the work and ensure that nothing throws
            await reader;
        }
#endif


        [Fact]
        public async Task Append_to_file_reads_all_data() {
            //write a file with a single row group
            var id = new DataField<int>("id");
            var ms = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 1, 2 }));
                }
            }

            //append to this file. Note that you cannot append to existing row group, therefore create a new one
            ms.Position = 0;
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms, append: true)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 3, 4 }));
                }
            }

            //check that this file now contains two row groups and all the data is valid
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(2, reader.RowGroupCount);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(2, rg.RowCount);
                    Assert.Equal(new int[] { 1, 2 }, (await rg.ReadColumnAsync(id)).Data);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(2, rg.RowCount);
                    Assert.Equal(new int[] { 3, 4 }, (await rg.ReadColumnAsync(id)).Data);
                }

            }
        }

        public readonly static IEnumerable<object[]> NullableColumnContentCases = new List<object[]>(){
            new object[] { new int?[] { 1, 2 } },
            new object[] { new int?[] { null } },
            new object[] { new int?[] { 1, null, 2 } } };

        [Theory]
        [MemberData(nameof(NullableColumnContentCases))]
        public async Task Write_read_nullable_column(Array input) {
            var id = new DataField<int?>("id");
            var ms = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, input));
                }
            }

            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(1, reader.RowGroupCount);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(input.Length, rg.RowCount);
                    Assert.Equal(input, (await rg.ReadColumnAsync(id)).Data);
                }
            }
        }

        [Fact]
        public async Task FileMetadata_sets_num_rows_on_file_and_row_group() {
            var ms = new MemoryStream();
            var id = new DataField<int>("id");

            //write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
                }
            }

            //read back
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(4, reader.Metadata!.NumRows);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(4, rg.RowCount);
                }
            }
        }

        [Fact]
        public async Task FileMetadata_sets_num_rows_on_file_and_row_group_multiple_RowGroups() {
            var ms = new MemoryStream();
            var id = new DataField<int>("id");

            //write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 5, 6 }));
                }
            }

            //read back
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(6, reader.Metadata!.NumRows);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(4, rg.RowCount);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(2, rg.RowCount);
                }
            }
        }

        [Fact]
        public async Task CustomMetadata_file_can_write_and_read() {
            var ms = new MemoryStream();
            var id = new DataField<int>("id");

            //write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                writer.CustomMetadata = new Dictionary<string, string> {
                    ["key1"] = "value1",
                    ["key2"] = "value2"
                };

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
                }
            }

            //read back
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal("value1", reader.CustomMetadata["key1"]);
                Assert.Equal("value2", reader.CustomMetadata["key2"]);
            }
        }

        [Fact]
        public async Task CustomMetadata_column_can_write_and_read() {
            var ms = new MemoryStream();
            var id = new DataField<int>("id");

            //write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }),
                        new Dictionary<string, string> {
                            ["key1"] = "value1",
                            ["key2"] = "value2"
                        });
                }
            }

            //read back
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
                Dictionary<string, string> kv = rgr.GetCustomMetadata(id);
                Assert.Equal(2, kv.Count);
                Assert.Equal("value1", kv["key1"]);
                Assert.Equal("value2", kv["key2"]);
            }
        }

        [Fact]
        public async Task Dictionary_encoding_applied_for_repeated_strings() {
            var str = new DataField<string>("s");
            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(str), ms)) {
                writer.CompressionMethod = CompressionMethod.None;

                var strings = new List<string>();
                strings.AddRange(Enumerable.Repeat("Please consider reporting this to the maintainers", 10000));
                strings.AddRange(Enumerable.Repeat("UnsupportedOperationException indicates that the requested operation cannot be performed", 10000));
                strings.AddRange(Enumerable.Repeat("The main reason behind the occurrence of this error is...", 10000));
                var strData = new DataColumn(str, strings.ToArray());

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(strData);
                }
            }

            // if it's less than 200kb it's fine!
            Assert.True(ms.Length < 200000, $"output size is {ms.Length}");

        }

        [Fact]
        public async Task Dictionary_encoding_can_be_turned_off() {
            var str = new DataField<string>("s");
            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(str), ms,
                new ParquetOptions { UseDictionaryEncoding = false})) {
                writer.CompressionMethod = CompressionMethod.None;
                var strings = new List<string>();
                strings.AddRange(Enumerable.Repeat("Please consider reporting this to the maintainers", 10000));
                strings.AddRange(Enumerable.Repeat("UnsupportedOperationException indicates that the requested operation cannot be performed", 10000));
                strings.AddRange(Enumerable.Repeat("The main reason behind the occurrence of this error is...", 10000));
                var strData = new DataColumn(str, strings.ToArray());

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(strData);
                }
            }

            // should be relatively big
            Assert.True(ms.Length > 200000, $"output size is {ms.Length}");

        }

        [Fact]
        public async Task Default_int32_is_delta_binary_packed() {
            var ms = new MemoryStream();
            var id = new DataField<int>("id");

            //write
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
                }
            }

            //read back
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
                Meta.ColumnChunk? cc = rgr.GetMetadata(id);
                Assert.NotNull(cc);
                Assert.Equal(Parquet.Meta.Encoding.DELTA_BINARY_PACKED, cc.MetaData!.Encodings[2]);
            }
        }

        [Fact]
        public async Task Override_int32_encoding_to_plain() {
            var ms = new MemoryStream();
            var id = new DataField<int>("id");

            //write
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms,
                new ParquetOptions { UseDeltaBinaryPackedEncoding = false })) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3, 4 }));
                }
            }

            //read back
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
                Meta.ColumnChunk? cc = rgr.GetMetadata(id);
                Assert.NotNull(cc);
                Assert.Equal(Parquet.Meta.Encoding.PLAIN, cc.MetaData!.Encodings[2]);
            }
        }
    }
}