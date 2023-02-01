using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings {
    public class BitPackedEncoderTest {

        const string Binary0to7WithBitWidth3 = "10001000 11000110 11111010";
        const string Binary0to7WithBitWidth3Legacy = "00000101 00111001 01110111";

        private byte[] BytesFromBinaryString(string s) {
            return s.Split(' ').Select(s => Convert.ToByte(s, 2)).ToArray();
        }

        [Fact]
        public void LegacyEncode1to7WithBitWidth3() {
            var dest = new List<byte>();
            BitPackedEncoder.LegacyEncode(new[] { 0, 1, 2, 3, 4, 5, 6, 7 }, 3, dest);

            Assert.Equal(BytesFromBinaryString(Binary0to7WithBitWidth3Legacy), dest.ToArray());
        }

        [Fact]
        public void LegacyDecode1to7WithBitWidth3() {
            int[] ints = new int[8];
            BitPackedEncoder.LegacyDecode(BytesFromBinaryString(Binary0to7WithBitWidth3Legacy).AsSpan(), 3, ints, 0, 8);
            Assert.Equal(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, ints);
        }

        [Fact]
        public void Encode1to7WithBitWidth3() {
            var dest = new List<byte>();
            BitPackedEncoder.Encode(new[] { 0, 1, 2, 3, 4, 5, 6, 7 }, 3, dest);

            Assert.Equal(BytesFromBinaryString(Binary0to7WithBitWidth3), dest.ToArray());
        }

        [Fact]
        public void Decode1to7WithBitWidth3() {
            Span<int> ints = new int[8];
            BitPackedEncoder.Decode(BytesFromBinaryString(Binary0to7WithBitWidth3).AsSpan(), 3, ints);
            Assert.Equal(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, ints.ToArray());
        }

    }
}
