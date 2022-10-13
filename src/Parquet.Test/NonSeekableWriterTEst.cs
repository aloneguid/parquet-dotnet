using System;
using System.IO;
using System.Threading.Tasks;
using NetBox.IO;
using Parquet.Data;
using Xunit;

namespace Parquet.Test {
    public class NonSeekableWriterTest {
        [Fact]
        public async Task Write_multiple_row_groups_to_forward_only_stream() {
            var ms = new MemoryStream();
            var forwardOnly = new WriteableNonSeekableStream(ms);

            var schema = new Schema(
               new DataField<int>("id"),
               new DataField<string>("nonsense"));

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, forwardOnly)) {
                using(ParquetRowGroupWriter rgw = writer.CreateRowGroup()) {
                    await rgw.WriteColumnAsync(new DataColumn((DataField)schema[0], new[] { 1 }));
                    await rgw.WriteColumnAsync(new DataColumn((DataField)schema[1], new[] { "1" }));
                }

                using(ParquetRowGroupWriter rgw = writer.CreateRowGroup()) {
                    await rgw.WriteColumnAsync(new DataColumn((DataField)schema[0], new[] { 2 }));
                    await rgw.WriteColumnAsync(new DataColumn((DataField)schema[1], new[] { "2" }));
                }
            }

            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                Assert.Equal(2, reader.RowGroupCount);

                using(ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(1, rgr.RowCount);

                    DataColumn column = await rgr.ReadColumnAsync((DataField)schema[0]);
                    Assert.Equal(1, column.Data.GetValue(0));
                }

                using(ParquetRowGroupReader rgr = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(1, rgr.RowCount);

                    DataColumn column = await rgr.ReadColumnAsync((DataField)schema[0]);
                    Assert.Equal(2, column.Data.GetValue(0));

                }

            }
        }

        public class WriteableNonSeekableStream : DelegatedStream {
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
}