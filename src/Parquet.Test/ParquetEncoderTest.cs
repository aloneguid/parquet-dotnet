using System;
using System.IO;
using Parquet.Encodings;
using Parquet.Meta;
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
            ParquetPlainEncoder.Decode(source.ToArray().AsSpan(), decoded.AsSpan(), new SchemaElement { Type = Meta.Type.INT64 });

            Assert.Equal(new TimeSpan(0, 0, 1), decoded[0]);
            Assert.Equal(new TimeSpan(0, 0, 30), decoded[1]);
            
#if NET6_0_OR_GREATER
            var decodedAsTimeOnly = new TimeOnly[2];
            ParquetPlainEncoder.Decode(source.ToArray().AsSpan(), decodedAsTimeOnly.AsSpan(), new SchemaElement { Type = Meta.Type.INT64 });
            Assert.Equal(new TimeOnly(0, 0, 1), decodedAsTimeOnly[0]);
            Assert.Equal(new TimeOnly(0, 0, 30), decodedAsTimeOnly[1]);
#endif
        }
    }
}
