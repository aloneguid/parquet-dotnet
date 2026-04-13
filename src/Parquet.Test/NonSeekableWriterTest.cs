using System;
using System.IO;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Test.Util;
using Xunit;

namespace Parquet.Test;

public class NonSeekableWriterTest {
    [Fact]
    public async Task Write_multiple_RowGroups_to_forward_only_stream() {
        var ms = new MemoryStream();
        var forwardOnly = new WriteableNonSeekableStream(ms);

        var schema = new ParquetSchema(
           new DataField<int>("id"),
           new DataField<string>("nonsense"));

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, forwardOnly)) {
            using(ParquetRowGroupWriter rgw = writer.CreateRowGroup()) {
                await rgw.WriteAsync<int>(schema.DataFields[0], new[] { 1 });
                await rgw.WriteAsync(schema.DataFields[1], ["1"]);
            }

            using(ParquetRowGroupWriter rgw = writer.CreateRowGroup()) {
                await rgw.WriteAsync<int>(schema.DataFields[0], new[] { 2 });
                await rgw.WriteAsync(schema.DataFields[1], ["2"]);
            }
        }

        ms.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
            Assert.Equal(2, reader.RowGroupCount);

            using(ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0)) {
                Assert.Equal(1, rgr.RowCount);

                int[] values = new int[checked((int)rgr.RowCount)];
                await rgr.ReadAsync<int>(schema.DataFields[0], values);
                Assert.Equal(1, values[0]);
            }

            using(ParquetRowGroupReader rgr = reader.OpenRowGroupReader(1)) {
                Assert.Equal(1, rgr.RowCount);

                int[] values = new int[checked((int)rgr.RowCount)];
                await rgr.ReadAsync<int>(schema.DataFields[0], values);
                Assert.Equal(2, values[0]);
            }

        }
    }

    class WriteableNonSeekableStream : DelegatedStream {
        public WriteableNonSeekableStream(Stream master) : base(master) {
        }

        public override bool CanSeek => false;

        public override bool CanRead => true;

        public override long Seek(long offset, SeekOrigin origin) {
            throw new NotSupportedException();
        }

        public override long Position {
            get => base.Position;
            set => throw new NotSupportedException();
        }
    }

}