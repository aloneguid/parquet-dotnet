using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test;

public class ParquetWriterTest : TestBase {

    [Fact]
    public async Task SimplestWrite() {
        var id = new DataField<int>("id");
        var ms = new MemoryStream();

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 1, 2, 3 });
            }
        }

        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal(1, reader.RowGroupCount);
            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                Assert.Equal(3, rg.RowCount);
                int[] values = new int[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal([1, 2, 3], values);
            }
        }
    }

    [Fact]
    public async Task Write_read_string_column_array_overload() {
        var name = new DataField<string>("name");
        string[] input = ["start", "stop", "pause"];
        var ms = new MemoryStream();

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(name), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync(name, input);
            }
        }

        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal(1, reader.RowGroupCount);
            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                Assert.Equal(input.Length, rg.RowCount);
                string[] values = new string[rg.RowCount];
                await rg.ReadAsync(name, values);
                Assert.Equal(input, values);
            }
        }
    }

    [Fact]
    public async Task Cannot_write_columns_in_wrong_order() {
        var schema = new ParquetSchema(new DataField<int>("id"), new DataField<int>("id2"));

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, new MemoryStream())) {

            using(ParquetRowGroupWriter gw = writer.CreateRowGroup()) {
                // Try to write second column first - should throw ArgumentException
                await Assert.ThrowsAsync<ArgumentException>(async () => {
                    await gw.WriteAsync<int>((DataField<int>)schema[1], new int[] { 1 });
                });
                // Now write columns in correct order to satisfy the validation
                await gw.WriteAsync<int>((DataField<int>)schema[0], new int[] { 1 });
                await gw.WriteAsync<int>((DataField<int>)schema[1], new int[] { 1 });
            }
        }
    }

    [Fact]
    public async Task Write_in_small_RowGroups() {
        //write a single file having 3 row groups
        var id = new DataField<int>("id");
        var ms = new MemoryStream();

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 1 });
            }

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 2 });
            }

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 3 });
            }

        }

        //read the file back and validate
        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal(3, reader.RowGroupCount);

            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                Assert.Equal(1, rg.RowCount);
                int[] values = new int[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal([1], values);
            }

            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                Assert.Equal(1, rg.RowCount);
                int[] values = new int[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal([2], values);
            }

            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(2)) {
                Assert.Equal(1, rg.RowCount);
                int[] values = new int[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal([3], values);
            }
        }
    }

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
            await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(3, reader.RowGroupCount);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(1, rg.RowCount);
                    int[] values = new int[rg.RowCount];
                    await rg.ReadAsync<int>(id, values);
                    Assert.Equal(new int[] { 1 }, values);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(1, rg.RowCount);
                    int[] values = new int[rg.RowCount];
                    await rg.ReadAsync<int>(id, values);
                    Assert.Equal(new int[] { 2 }, values);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(2)) {
                    Assert.Equal(1, rg.RowCount);
                    int[] values = new int[rg.RowCount];
                    await rg.ReadAsync<int>(id, values);
                    Assert.Equal(new int[] { 3 }, values);
                }
            }
        });

        // actual writing now that the reader is set up
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ws)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 1 });
            }

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 2 });
            }

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 3 });
            }
        }
        ws.Dispose();

        //run the work and ensure that nothing throws
        await reader;
    }


    [Fact]
    public async Task Append_to_file_reads_all_data() {
        //write a file with a single row group
        var id = new DataField<int>("id");
        var ms = new MemoryStream();

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 1, 2 });
            }
        }

        //append to this file. Note that you cannot append to existing row group, therefore create a new one
        ms.Position = 0;
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms, append: true)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 3, 4 });
            }
        }

        //check that this file now contains two row groups and all the data is valid
        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal(2, reader.RowGroupCount);

            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                Assert.Equal(2, rg.RowCount);
                int[] values = new int[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal(new int[] { 1, 2 }, values);
            }

            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                Assert.Equal(2, rg.RowCount);
                int[] values = new int[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal(new int[] { 3, 4 }, values);
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

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, (int?[])input);
            }
        }

        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal(1, reader.RowGroupCount);

            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                Assert.Equal(input.Length, rg.RowCount);
                int?[] values = new int?[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal(input, values);
            }
        }
    }

    [Theory]
    [MemberData(nameof(NullableColumnContentCases))]
    public async Task Write_read_nullable_column_array_overload(Array input) {
        var id = new DataField<int?>("id");
        var ms = new MemoryStream();

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, (int?[])input);
                rg.CompleteValidate();
            }
        }

        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal(1, reader.RowGroupCount);

            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                Assert.Equal(input.Length, rg.RowCount);
                int?[] values = new int?[rg.RowCount];
                await rg.ReadAsync<int>(id, values);
                Assert.Equal(input, values);
            }
        }
    }

    [Fact]
    public async Task FileMetadata_sets_num_rows_on_file_and_row_group() {
        var ms = new MemoryStream();
        var id = new DataField<int>("id");

        //write
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new[] { 1, 2, 3, 4 });
            }
        }

        //read back
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
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
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new[] { 1, 2, 3, 4 });
            }

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new[] { 5, 6 });
            }
        }

        //read back
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
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
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            writer.CustomMetadata = new Dictionary<string, string> {
                ["key1"] = "value1",
                ["key2"] = "value2"
            };

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new[] { 1, 2, 3, 4 });
            }
        }

        //read back
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal("value1", reader.CustomMetadata["key1"]);
            Assert.Equal("value2", reader.CustomMetadata["key2"]);
        }
    }

    [Fact]
    public async Task CustomMetadata_column_can_write_and_read() {
        var ms = new MemoryStream();
        var id = new DataField<int>("id");

        //write
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new int[] { 1, 2, 3, 4 },
                    null,
                    new Dictionary<string, string> {
                        ["key1"] = "value1",
                        ["key2"] = "value2"
                    });
            }
        }

        //read back
        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
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

        var options = new ParquetOptions();
        options.DictionaryEncodedColumns.Add(str.Path.ToString());

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(str), ms, options)) {
            writer.CompressionMethod = CompressionMethod.None;

            var strings = new List<string>();
            strings.AddRange(Enumerable.Repeat("Please consider reporting this to the maintainers", 10000));
            strings.AddRange(Enumerable.Repeat("UnsupportedOperationException indicates that the requested operation cannot be performed", 10000));
            strings.AddRange(Enumerable.Repeat("The main reason behind the occurrence of this error is...", 10000));

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync(str, strings);
            }
        }

        // if it's less than 200kb it's fine!
        Assert.True(ms.Length < 200000, $"output size is {ms.Length}");

    }

    [Fact]
    public async Task Dictionary_encoding_can_be_turned_off() {
        var str = new DataField<string>("s");
        using var ms = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(str), ms,
            new ParquetOptions { UseDictionaryEncoding = false })) {
            writer.CompressionMethod = CompressionMethod.None;
            var strings = new List<string>();
            strings.AddRange(Enumerable.Repeat("Please consider reporting this to the maintainers", 10000));
            strings.AddRange(Enumerable.Repeat("UnsupportedOperationException indicates that the requested operation cannot be performed", 10000));
            strings.AddRange(Enumerable.Repeat("The main reason behind the occurrence of this error is...", 10000));
            var strData = new DataColumn(str, strings.ToArray());

            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync(str, strings.ToArray());
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
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms)) {
            using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                await rg.WriteAsync<int>(id, new[] { 1, 2, 3, 4 });
            }
        }

        //read back
        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            Meta.ColumnChunk? cc = rgr.GetMetadata(id);
            Assert.NotNull(cc);
            Assert.Contains(Parquet.Meta.Encoding.DELTA_BINARY_PACKED, cc.MetaData!.Encodings);
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
                await rg.WriteAsync<int>(id, new[] { 1, 2, 3, 4 });
            }
        }

        //read back
        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            Meta.ColumnChunk? cc = rgr.GetMetadata(id);
            Assert.NotNull(cc);
            Assert.Contains(Parquet.Meta.Encoding.PLAIN, cc.MetaData!.Encodings);
        }
    }

    [Fact]
    public async Task Cannot_write_more_columns_than_schema() {
        var schema = new ParquetSchema(new DataField<int>("id"));

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, new MemoryStream())) {
            await Assert.ThrowsAsync<InvalidOperationException>(async () => {
                using(ParquetRowGroupWriter gw = writer.CreateRowGroup()) {
                    // Write the first column successfully
                    await gw.WriteAsync<int>((DataField<int>)schema[0], new int[] { 1 });

                    // Attempting to write the same column again (exceeding schema) should throw
                    await gw.WriteAsync<int>((DataField<int>)schema[0], new int[] { 2 });
                }
            });
        }
    }

    [Fact]
    public async Task Must_write_all_columns_from_schema() {
        var schema = new ParquetSchema(new DataField<int>("id"), new DataField<int>("value"));

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, new MemoryStream())) {
            Assert.Throws<InvalidOperationException>(() => {
                using(ParquetRowGroupWriter gw = writer.CreateRowGroup()) {
                    // Write only the first column, missing the second one
                    gw.WriteAsync<int>((DataField<int>)schema[0], new int[] { 1 }).Wait();
                    // "Dispose" will not throw since v5.4.0
                    gw.CompleteValidate();
                }
            });
        }
    }

    [Fact]
    public async Task All_row_groups_must_have_same_columns() {
        var schema = new ParquetSchema(new DataField<int>("id"), new DataField<int>("value"));

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, new MemoryStream())) {
            // First row group - write all columns correctly
            using(ParquetRowGroupWriter gw = writer.CreateRowGroup()) {
                await gw.WriteAsync<int>((DataField<int>)schema[0], new int[] { 1 });
                await gw.WriteAsync<int>((DataField<int>)schema[1], new int[] { 100 });
            }

            // Second row group - attempt to write only one column
            Assert.Throws<InvalidOperationException>(() => {
                using(ParquetRowGroupWriter gw = writer.CreateRowGroup()) {
                    gw.WriteAsync<int>((DataField<int>)schema[0], new int[] { 2 }).Wait();
                    // "Dispose" will not throw since v5.4.0
                    gw.CompleteValidate();
                }
            });
        }
    }

    [Fact]
    public async Task All_columns_must_have_same_row_count() {
        var schema = new ParquetSchema(new DataField<int>("id"), new DataField<int>("value"));

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, new MemoryStream())) {
            await Assert.ThrowsAsync<InvalidOperationException>(async () => {
                using(ParquetRowGroupWriter gw = writer.CreateRowGroup()) {
                    // First column has 3 rows
                    await gw.WriteAsync<int>((DataField<int>)schema[0], new int[] { 1, 2, 3 });
                    // Second column has 2 rows - should throw
                    await gw.WriteAsync<int>((DataField<int>)schema[1], new int[] { 100, 200 });
                }
            });
        }
    }
}