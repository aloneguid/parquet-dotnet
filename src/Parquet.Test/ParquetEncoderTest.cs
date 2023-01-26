using System;
using System.IO;
using Parquet.Data;
using Xunit;

namespace Parquet.Test {
    public class ParquetEncoderTest {

        [Fact]
        public void DecodeOutOfRangeSpan() {
            var source = new MemoryStream(new byte[] {
                64,  66,  15,  0, 0, 0, 0, 0,
                128, 195, 201, 1, 0, 0, 0, 0,
                1,   1,   1,   1, 1, 1, 1, 1, 1 });

            var decoded = new TimeSpan[2];
            ParquetPlainEncoder.Decode(source, decoded.AsSpan(), new Thrift.SchemaElement { Type = Thrift.Type.INT64 });

            Assert.Equal(new TimeSpan(0, 0, 1), decoded[0]);
            Assert.Equal(new TimeSpan(0, 0, 30), decoded[1]);
        }
    }
}
