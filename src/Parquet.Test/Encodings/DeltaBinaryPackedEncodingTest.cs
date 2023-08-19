using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings {
    public class DeltaBinaryPackedEncodingTest {
        [Fact]
        public void EncodeAndDecodeInt32() {

            int[] input = new[] { 7, 5, 3, 1, -2, 3, 4, -5, };

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecodeInt32_Max() {

            int[] input = new[] { int.MaxValue, int.MaxValue - 5, int.MaxValue - 3, int.MaxValue - 1, int.MaxValue - 3, int.MaxValue - 4, };

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecodeInt32_Min() {

            int[] input = new[] { int.MinValue, int.MinValue + 5, int.MinValue + 3, int.MinValue + 1, int.MinValue + 3, int.MinValue + 4, };

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }
        [Fact]
        public void EncodeAndDecodeInt32_1_100000() {
            int total = 100000;
            int[] input = Enumerable.Range(1, total).ToArray();

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecodeInt64() {
            long[] input = new[] { 7L, 5L, 3L, 1L, -2L, 3L, 4L, -5L, };

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecodeInt64_Max() {

            long[] input = new[] { long.MaxValue, long.MaxValue - 5, long.MaxValue - 3, long.MaxValue - 1, long.MaxValue - 3, long.MaxValue - 4, };

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecodeInt64_Min() {

            long[] input = new[] { long.MinValue, long.MinValue + 5, long.MinValue + 3, long.MinValue + 1, long.MinValue + 3, long.MinValue + 4, };

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }


        [Fact]
        public void EncodeAndDecodeInt64_1_100000() {
            int total = 100000;
            long[] input = Enumerable.Range(1, total).Select(i => (long)i).ToArray();

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            long[] des = new long[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecodeInt32_Random_Overflow() {
            var r = new Random(0);
            int total = 1000;
            int[] input = Enumerable.Range(0, total).Select(i => r.Next(int.MinValue, int.MaxValue)).ToArray();

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            int[] des = new int[input.Length];
            int i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }

        [Fact]
        public void EncodeAndDecodeInt64_Random_Overflow() {
            var r = new Random(0);
            int total = 1000;
            long[] input = Enumerable.Range(0, total).Select(i => {
                byte[] buffer = new byte[8];
                r.NextBytes(buffer);
                long randomInt64 = BitConverter.ToInt64(buffer, 0);
                return randomInt64;
            }).ToArray();

            using var ms = new MemoryStream();
            DeltaBinaryPackedEncoder.Encode(input, 0, input.Length, ms);

            long[] des = new long[input.Length];
            long i = DeltaBinaryPackedEncoder.Decode(ms.ToArray(), des, 0, input.Length, out int b);

            Assert.Equal(input, des);
        }
    }
}
