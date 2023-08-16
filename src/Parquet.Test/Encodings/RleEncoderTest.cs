using System;
using System.IO;
using System.Linq;
using Parquet.Encodings;
using Parquet.Extensions;
using Xunit;

namespace Parquet.Test.Encodings {
    public class RleEncoderTest {
        [Theory]
        [InlineData(new int[] { 1, 2, 3, 3, 3, 3 })]
        [InlineData(new int[] { 0 })]
        public void EncodeDecodeTest(int[] input) {

            int bitWidth = input.Max().GetBitWidth();
            using var ms = new MemoryStream();

            // encode
            RleBitpackedHybridEncoder.Encode(ms, input.AsSpan(), bitWidth);

            //decode
            ms.Position = 0;
            int[] r2 = new int[input.Length];
            RleBitpackedHybridEncoder.Decode(ms.ToArray().AsSpan(), bitWidth, r2.Length, out _, r2.AsSpan(), r2.Length);

            Assert.Equal(input, r2);
        }

        [Fact]
        public void Encode0to7() {
            using var ms = new MemoryStream();
            RleBitpackedHybridEncoder.Encode(ms, new[] { 0, 1, 2, 3, 4, 5, 6, 7 }.AsSpan(), 3);
            byte[] ebytes = ms.ToArray();
        }

        [Fact]
        public void EncodeBitWidth10() {
            using var ms = new MemoryStream();
            const int count = 1000;
            int bitWidth = count.GetBitWidth();
            Assert.Equal(10, bitWidth);
            int[] data = Enumerable.Range(0, count).ToArray();

            RleBitpackedHybridEncoder.Encode(ms, data.AsSpan(), bitWidth);

            ms.Position = 0;
            int[] data2 = new int[count];
            int count2 = RleBitpackedHybridEncoder.Decode(ms.ToArray().AsSpan(), bitWidth, ms.ToArray().Length, out _, data2.AsSpan(), count);

            Assert.Equal(data, data2);

        }

        [Fact]
        public void EncodeBitWidth18() {
            using var ms = new MemoryStream();
            const int count = 180000;
            int bitWidth = count.GetBitWidth();
            Assert.Equal(18, bitWidth);
            int[] data = Enumerable.Range(0, count).ToArray();

            RleBitpackedHybridEncoder.Encode(ms, data.AsSpan(), bitWidth);

            ms.Position = 0;
            int[] data2 = new int[count];
            int count2 = RleBitpackedHybridEncoder.Decode(ms.ToArray().AsSpan(), bitWidth, ms.ToArray().Length, out _, data2.AsSpan(), count);

            Assert.Equal(data, data2);

        }
    }
}
