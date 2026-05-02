using System;
using System.Linq;
using System.Numerics;
using Parquet.Data;
using Xunit;

namespace Parquet.Test.DataTypes;

public class BigDecimalTest {
    [Fact]
    public void Parse_from_decimal() {
        var bd = BigDecimal.FromDecimal(83086059037282.54m, 38, 16);
        Assert.Equal("83086059037282.5400000000000000", bd.ToString());
    }

    [Fact]
    public void Parse_from_BigInteger() {
        var bigInt = BigInteger.Parse("12345678901234567890123456789012345678");
        var bd = new BigDecimal(bigInt, 38, 2);
        Assert.Equal("123456789012345678901234567890123456.78", bd.ToString());
    }

    /// <summary>
    /// Comprehensive roundtrip: construct BigDecimal → GetBytes (big-endian) → reverse → reconstruct BigInteger.
    /// Covers positive, negative, zero, small, large, various precisions and scales.
    /// </summary>
    [Theory]
    // zero
    [InlineData("0", 10, 0)]
    [InlineData("0", 10, 5)]
    [InlineData("0", 38, 18)]
    // small positive, no scale
    [InlineData("1", 1, 0)]
    [InlineData("9", 1, 0)]
    [InlineData("42", 5, 0)]
    [InlineData("127", 5, 0)]
    [InlineData("255", 5, 0)]
    [InlineData("256", 5, 0)]
    [InlineData("65535", 10, 0)]
    // small positive, with scale
    [InlineData("12345", 10, 2)]      // 123.45
    [InlineData("100", 10, 2)]        // 1.00
    [InlineData("1", 10, 5)]          // 0.00001
    [InlineData("999999", 10, 4)]     // 99.9999
    // small negative, no scale
    [InlineData("-1", 1, 0)]
    [InlineData("-9", 1, 0)]
    [InlineData("-42", 5, 0)]
    [InlineData("-128", 5, 0)]
    [InlineData("-256", 5, 0)]
    [InlineData("-65535", 10, 0)]
    // small negative, with scale
    [InlineData("-12345", 10, 2)]     // -123.45
    [InlineData("-100", 10, 2)]       // -1.00
    [InlineData("-1", 10, 5)]         // -0.00001
    // medium positive
    [InlineData("12345678901234", 18, 4)]
    [InlineData("999999999999999999", 18, 0)]
    // medium negative
    [InlineData("-12345678901234", 18, 4)]
    [InlineData("-999999999999999999", 18, 0)]
    // large positive (precision 28, 12 bytes)
    [InlineData("1234567890123456789012", 28, 6)]
    [InlineData("9999999999999999999999999999", 28, 0)]
    // large negative (precision 28, 12 bytes)
    [InlineData("-1234567890123456789012", 28, 6)]
    [InlineData("-9999999999999999999999999999", 28, 0)]
    // max positive (precision 38, 16 bytes)
    [InlineData("12345678901234567890123456789012345678", 38, 2)]
    [InlineData("99999999999999999999999999999999999999", 38, 0)]
    [InlineData("99999999999999999999999999999999999", 38, 5)]
    // max negative (precision 38, 16 bytes)
    [InlineData("-12345678901234567890123456789012345678", 38, 2)]
    [InlineData("-99999999999999999999999999999999999999", 38, 0)]
    [InlineData("-99999999999999999999999999999999999", 38, 5)]
    // high scale
    [InlineData("123456789012345678", 38, 20)]
    [InlineData("-123456789012345678", 38, 20)]
    [InlineData("1", 38, 37)]
    [InlineData("-1", 38, 37)]
    public void GetBytes_roundtrip_preserves_value(string unscaledStr, int precision, int scale) {
        BigInteger unscaled = BigInteger.Parse(unscaledStr);
        var bd = new BigDecimal(unscaled, precision, scale);

        byte[] bytes = bd.GetBytes();

        // GetBytes returns big-endian; reverse to little-endian for BigInteger
        byte[] littleEndian = bytes.Reverse().ToArray();
        BigInteger decoded = new BigInteger(littleEndian);
        Assert.Equal(unscaled, decoded);
    }

    /// <summary>
    /// Verifies that GetBufferSize returns the correct byte count for each precision.
    /// </summary>
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(3, 2)]
    [InlineData(4, 2)]
    [InlineData(5, 3)]
    [InlineData(9, 4)]
    [InlineData(10, 5)]
    [InlineData(18, 8)]
    [InlineData(28, 12)]
    [InlineData(38, 16)]
    public void WriteBytes_returns_correct_size_for_precision(int precision, int expectedSize) {
        var bd = new BigDecimal(BigInteger.One, precision, 0);
        byte[] buffer = new byte[16];

        int written = bd.WriteBytes(buffer.AsSpan());

        Assert.Equal(expectedSize, written);
    }

    /// <summary>
    /// Zero must encode to all-zero bytes regardless of precision/scale.
    /// </summary>
    [Theory]
    [InlineData(1, 0)]
    [InlineData(10, 2)]
    [InlineData(38, 18)]
    public void WriteBytes_zero_produces_all_zero_bytes(int precision, int scale) {
        var bd = new BigDecimal(BigInteger.Zero, precision, scale);
        byte[] buffer = new byte[16];
        Array.Fill(buffer, (byte)0xFF);

        int written = bd.WriteBytes(buffer.AsSpan());

        for(int i = 0; i < written; i++) {
            Assert.Equal(0, buffer[i]);
        }
    }

    /// <summary>
    /// Negative values must have 0xFF sign-extension in the leading (big-endian) bytes.
    /// </summary>
    [Theory]
    [InlineData("-1", 10, 0)]
    [InlineData("-100", 10, 2)]
    [InlineData("-12345678901234567890", 38, 2)]
    public void WriteBytes_negative_value_has_sign_extension(string unscaledStr, int precision, int scale) {
        var bd = new BigDecimal(BigInteger.Parse(unscaledStr), precision, scale);
        byte[] buffer = new byte[16];

        int written = bd.WriteBytes(buffer.AsSpan());

        // First byte in big-endian must have high bit set (>= 0x80) for negative values
        Assert.True(buffer[0] >= 0x80,
            $"Leading byte should have high bit set for negative value {unscaledStr}, got 0x{buffer[0]:X2}");
    }

    /// <summary>
    /// Positive values must NOT have the sign bit set in the leading byte.
    /// </summary>
    [Theory]
    [InlineData("1", 10, 0)]
    [InlineData("12345", 10, 2)]
    [InlineData("12345678901234567890", 38, 2)]
    public void WriteBytes_positive_value_has_no_sign_bit(string unscaledStr, int precision, int scale) {
        var bd = new BigDecimal(BigInteger.Parse(unscaledStr), precision, scale);
        byte[] buffer = new byte[16];

        int written = bd.WriteBytes(buffer.AsSpan());

        // First byte in big-endian must have high bit clear (< 0x80) for positive values
        Assert.True(buffer[0] < 0x80,
            $"Leading byte should not have sign bit for positive value {unscaledStr}, got 0x{buffer[0]:X2}");
    }

    [Fact]
    public void WriteBytes_buffer_too_small_throws() {
        var bd = new BigDecimal(BigInteger.Parse("12345"), 38, 2);
        byte[] tooSmallArray = new byte[8];

        ArgumentException ex = Assert.Throws<ArgumentException>(() => bd.WriteBytes(tooSmallArray.AsSpan()));
        Assert.Contains("destination buffer", ex.Message);
    }

    /// <summary>
    /// ToString must correctly format for a matrix of values.
    /// </summary>
    [Theory]
    [InlineData("12345", 10, 2, "123.45")]
    [InlineData("-12345", 10, 2, "-123.45")]
    [InlineData("0", 10, 2, "0.00")]
    [InlineData("100", 10, 2, "1.00")]
    [InlineData("-100", 10, 2, "-1.00")]
    [InlineData("1", 10, 5, "0.00001")]
    [InlineData("-1", 10, 5, "-0.00001")]
    [InlineData("42", 5, 0, "42")]
    [InlineData("-42", 5, 0, "-42")]
    [InlineData("12345678901234567890123456789012345678", 38, 2, "123456789012345678901234567890123456.78")]
    [InlineData("-12345678901234567890123456789012345678", 38, 2, "-123456789012345678901234567890123456.78")]
    public void ToString_formats_correctly(string unscaledStr, int precision, int scale, string expected) {
        var bd = new BigDecimal(BigInteger.Parse(unscaledStr), precision, scale);
        Assert.Equal(expected, bd.ToString());
    }
}
