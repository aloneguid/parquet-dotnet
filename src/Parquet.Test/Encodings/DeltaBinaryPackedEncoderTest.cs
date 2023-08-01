using System.IO;
using System.Linq;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings {
    public class DeltaBinaryPackedEncoderTest {

        [Fact]
        public void EncodeAndDecode() {

            int[] input = new[] { 7, 5, 3, 1, -2, 3, 4, -5, };

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }


        [Fact]
        public void LongEncodeAndDecode() {
            long[] input = new[] { 7L, 5L, 3L, 1L, -2L, 3L, 4L, -5L, };

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecode3() {

            int[] input = new[] { int.MaxValue - 7, int.MaxValue - 5, int.MaxValue - 3, int.MaxValue - 1, int.MaxValue - 3, int.MaxValue - 4, };
            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void LongEncodeAndDecode3() {

            long[] input = new[] { long.MaxValue - 7, long.MaxValue - 5, long.MaxValue - 3, long.MaxValue - 1, long.MaxValue - 3, long.MaxValue - 4, };

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecode4() {

            int[] input = new[] { int.MaxValue, int.MaxValue - 5, int.MaxValue - 3, int.MaxValue - 1, int.MaxValue - 3, int.MaxValue - 4, };

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void LongEncodeAndDecode4() {

            long[] input = new[] { long.MaxValue, long.MaxValue - 5, long.MaxValue - 3, long.MaxValue - 1, long.MaxValue - 3, long.MaxValue - 4, };

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecode5() {

            int[] input = new[] { int.MinValue, int.MinValue + 5, int.MinValue + 3, int.MinValue + 1, int.MinValue + 3, int.MinValue + 4, };

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void LongEncodeAndDecode5() {

            long[] input = new[] { long.MinValue, long.MinValue + 5, long.MinValue + 3, long.MinValue + 1, long.MinValue + 3, long.MinValue + 4, };

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecode6() {
            int total = 100000;
            int[] input = Enumerable.Range(1, total).ToArray();

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void LongEncodeAndDecode6() {
            int total = 100000;
            long[] input = Enumerable.Range(1, total).Select(i => (long)i).ToArray();

            var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }
    }
}
