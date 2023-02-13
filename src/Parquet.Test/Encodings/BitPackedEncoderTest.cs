using System;
using System.Linq;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings {
    public class BitPackedEncoderTest {

        const string Binary0to7WithBitWidth3LE = "10001000 11000110 11111010";
        const string Binary0to7WithBitWidth3BE = "00000101 00111001 01110111";

        private byte[] BytesFromBinaryString(string s) {
            return s.Split(' ').Select(s => Convert.ToByte(s, 2)).ToArray();
        }

        [Fact]
        public void Encode1to7WithBitWidth3BE() {
            byte[] dest = new byte[3];
            BitPackedEncoder.Encode8ValuesBE(new[] { 0, 1, 2, 3, 4, 5, 6, 7 }, dest, 3);

            Assert.Equal(BytesFromBinaryString(Binary0to7WithBitWidth3BE), dest.ToArray());
        }

        [Fact]
        public void Decode1to7WithBitWidthBE() {
            Span<int> ints = new int[8];
            BitPackedEncoder.Decode8ValuesBE(BytesFromBinaryString(Binary0to7WithBitWidth3BE).AsSpan(), ints, 3);
            Assert.Equal(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, ints.ToArray());
        }

        [Fact]
        public void Encode1to7WithBitWidth3() {
            byte[] dest = new byte[3];
            BitPackedEncoder.Encode8ValuesLE(new[] { 0, 1, 2, 3, 4, 5, 6, 7 }, dest, 3);

            Assert.Equal(BytesFromBinaryString(Binary0to7WithBitWidth3LE), dest.ToArray());
        }

        [Fact]
        public void Decode1to7WithBitWidth3() {
            Span<int> ints = new int[8];
            BitPackedEncoder.Decode8ValuesLE(BytesFromBinaryString(Binary0to7WithBitWidth3LE).AsSpan(), ints, 3);
            Assert.Equal(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, ints.ToArray());
        }

    }
}
