using System.IO;
using System.Linq;
using Parquet.Data;
using Xunit;

namespace Parquet.Test {
    public class RleEncoderTest {
        [Theory]
        [InlineData(new int[] {1, 2, 3, 3, 3, 3})]
        [InlineData(new int[] { 0 })]
        public void EncodeDecodeTest(int[] input) {

            int bitWidth = input.Max().GetBitWidth();
            using var ms = new MemoryStream();

            // encode
            RleEncoder.Encode(ms, input, input.Length, bitWidth);

            //decode
            ms.Position= 0;
            int[] r2 = new int[input.Length];
            RleEncoder.Decode(ms, r2, 0, bitWidth, r2.Length);

            Assert.Equal(input, r2);
        }

        [Fact]
        public void Encode0to7() {
            using var ms = new MemoryStream();
            RleEncoder.Encode(ms, new[] { 0, 1, 2, 3, 4, 5, 6, 7 }, 8, 3);
            byte[] ebytes = ms.ToArray();
        }

        [Fact]
        public void EncodeBitWidth10() {
            using var ms = new MemoryStream();
            const int count = 1000;
            int bitWidth = count.GetBitWidth();
            Assert.Equal(10, bitWidth);
            int[] data = Enumerable.Range(0, count).ToArray();

            RleEncoder.Encode(ms, data, count, bitWidth);

            ms.Position = 0;
            int[] data2 = new int[count];
            int count2 = RleEncoder.Decode(ms, data2, 0, bitWidth, count);

            Assert.Equal(data, data2);

        }

        [Fact]
        public void EncodeBitWidth18() {
            using var ms = new MemoryStream();
            const int count = 180000;
            int bitWidth = count.GetBitWidth();
            Assert.Equal(18, bitWidth);
            int[] data = Enumerable.Range(0, count).ToArray();

            RleEncoder.Encode(ms, data, count, bitWidth);

            ms.Position = 0;
            int[] data2 = new int[count];
            int count2 = RleEncoder.Decode(ms, data2, 0, bitWidth, count);

            Assert.Equal(data, data2);

        }
    }
}
