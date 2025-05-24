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

        [Fact]
        public void DecodeOversizedSpan() {
            var source = new MemoryStream(new byte[] { 
                1, 0, 0, 0, 
                2, 0, 0, 0, 
                0 //<- bad byte of trailing data in source
            });

            //We should still be able to read the data and ignore the extra byte
            int[] decoded = new int[2];
            ParquetPlainEncoder.Decode(source.ToArray().AsSpan(), decoded.AsSpan());

            Assert.Equal(1, decoded[0]);
            Assert.Equal(2, decoded[1]);
        }

        [Fact]
        public void Decode_ByteArrays_WithEmptyArrays() {
            var source = new MemoryStream();
            
            source.Write(BitConverter.GetBytes(3), 0, 4);
            source.Write([1, 2, 3], 0, 3);
            source.Write(BitConverter.GetBytes(0), 0, 4);
            source.Write(BitConverter.GetBytes(2), 0, 4);
            source.Write([4, 5], 0, 2);

            byte[][] decoded = new byte[3][];
            int elementsRead = ParquetPlainEncoder.Decode(
                source.ToArray().AsSpan(),
                decoded.AsSpan(),
                new SchemaElement { Type = Meta.Type.BYTE_ARRAY });

            Assert.Equal(3, elementsRead);
            Assert.Equal(new byte[] { 1, 2, 3 }, decoded[0]);
            Assert.NotNull(decoded[1]);
            Assert.Empty(decoded[1]);
            Assert.Equal(new byte[] { 4, 5 }, decoded[2]);
        }
    }
}
