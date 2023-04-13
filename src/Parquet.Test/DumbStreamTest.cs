using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class DumbStreamTest {
        [Fact]
        public async Task Read_from_stream_that_only_retuns_one_byte_at_the_time() {
            DataField field = new("date", typeof(DateTime));
            ParquetSchema schema = new(field);

            using MemoryStream memoryFileStream = new();
            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, memoryFileStream)) {
                using ParquetRowGroupWriter rowGroupWriter = parquetWriter.CreateRowGroup();
                DateTime[] data = new DateTime[10000];
                for(int i = 0; i < 10000; i++)
                    data[i] = DateTime.UtcNow.AddMilliseconds(i);
                await rowGroupWriter.WriteColumnAsync(new DataColumn(field, data));
            }

            int fileSize = (int)memoryFileStream.Length;
            using MemoryStream memoryStreamCopy = new(memoryFileStream.GetBuffer(), 0, fileSize);
            using DumbStream bufferedStream = new(memoryStreamCopy);
            using ParquetReader parquetReader = await ParquetReader.CreateAsync(bufferedStream);
            for(int iterations = 0; iterations < 100; iterations++) {
                using ParquetRowGroupReader rowGroupReader = parquetReader.OpenRowGroupReader(0);
                await rowGroupReader.ReadColumnAsync(field);
            }
        }

        private class DumbStream : Stream {
            private readonly MemoryStream _memoryStream;
            public DumbStream(MemoryStream memoryStream) { _memoryStream = memoryStream; }
            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite => false;
            public override long Length => _memoryStream.Length;
            public override long Position { get => _memoryStream.Position; set => _memoryStream.Position = value; }
            public override void Flush() { }
            public override int Read(byte[] buffer, int offset, int count) => _memoryStream.Read(buffer, offset, 1);
            public override long Seek(long offset, SeekOrigin origin) => _memoryStream.Seek(offset, origin);
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => Task.FromResult(Read(buffer, offset, count));
        }

    }
}