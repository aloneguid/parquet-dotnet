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
    }
}
