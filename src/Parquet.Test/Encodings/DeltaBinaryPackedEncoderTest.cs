using System.Linq;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings {
    public class DeltaBinaryPackedEncoderTest {

        [Fact]
        public void EncodeAndDecode() {


            int[] input = new[] { 7, 5, 3, 1, 2, 3, 4, 5, };
            byte[] encodedBytes = DeltaBinaryPackedEncoder.EncodeINT32(input);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(encodedBytes, des, 0, input.Length, out int b);

            Assert.Equal(input,des);
        }
    }
}
