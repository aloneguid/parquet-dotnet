using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Xunit;

namespace Parquet.Test.DataTypes;

public class VariantMetadataTest {
    [Fact]
    public void Parse_populated_dictionary() {
        var metadata = new VariantMetadata(CreateMetadata(1, false, false, "alpha", "beta", "gamma"));

        Assert.Equal(1, metadata.Version);
        Assert.False(metadata.SortedStrings);
        Assert.False(metadata.IsSortedStrings);
        Assert.Equal(1, metadata.OffsetSize);
        Assert.Equal(3, metadata.DictionarySize);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, metadata.Dictionary);
        Assert.Equal("beta", metadata[1]);
    }

    [Theory]
    [InlineData(1, 3)]
    [InlineData(2, 256)]
    [InlineData(3, 65536)]
    [InlineData(4, 65536)]
    public void Parse_all_offset_widths(int offsetSize, int valueLength) {
        string value = new('x', valueLength);
        var metadata = new VariantMetadata(CreateMetadata(offsetSize, false, false, value));

        Assert.Equal(1, metadata.Version);
        Assert.Equal(offsetSize, metadata.OffsetSize);
        Assert.Equal(1, metadata.DictionarySize);
        Assert.Equal(value, metadata[0]);
    }

    [Fact]
    public void Parse_empty_dictionary() {
        var metadata = new VariantMetadata(CreateMetadata(1, false, false));

        Assert.Equal(0, metadata.DictionarySize);
        Assert.Empty(metadata.Dictionary);
    }

    [Fact]
    public void Reserved_header_bit_is_ignored() {
        var metadata = new VariantMetadata(CreateMetadata(2, false, true, "value"));

        Assert.Equal(2, metadata.OffsetSize);
        Assert.Equal("value", metadata[0]);
    }

    [Fact]
    public void Unsorted_dictionary_preserves_duplicates_and_order() {
        var metadata = new VariantMetadata(CreateMetadata(1, false, false, "z", "a", "z"));

        Assert.Equal(new[] { "z", "a", "z" }, metadata.Dictionary);
    }

    [Fact]
    public void Sorted_dictionary_accepts_utf8_byte_order() {
        var metadata = new VariantMetadata(CreateMetadata(1, true, false, "a", "é", "😀"));

        Assert.Equal(new[] { "a", "é", "😀" }, metadata.Dictionary);
    }

    [Fact]
    public void Rejects_empty_and_truncated_metadata() {
        AssertMalformed(Array.Empty<byte>(), "header");
        AssertMalformed(new byte[] { 0x02 }, "version");
        AssertMalformed(new byte[] { 0x01 }, "dictionary size");
        AssertMalformed(new byte[] { 0x01, 0x01 }, "offset table");
    }

    [Fact]
    public void Rejects_invalid_offsets() {
        byte[] nonzeroFirst = CreateMetadata(1, false, false, "a");
        nonzeroFirst[2] = 1;
        AssertMalformed(nonzeroFirst, "first");

        byte[] decreasing = CreateMetadata(1, false, false, "a", "b");
        decreasing[3] = 2;
        decreasing[4] = 1;
        AssertMalformed(decreasing, "nondecreasing");

        byte[] outOfRange = CreateMetadata(1, false, false, "a");
        outOfRange[3] = 2;
        AssertMalformed(outOfRange, "exceeds");

        byte[] finalMismatch = CreateMetadata(1, false, false, "a", "b");
        finalMismatch[4] = 1;
        AssertMalformed(finalMismatch, "final");
    }

    [Fact]
    public void Rejects_invalid_utf8_and_sorted_order_violations() {
        byte[] invalidUtf8 = CreateMetadata(1, false, false, "a");
        invalidUtf8[^1] = 0xFF;
        AssertMalformed(invalidUtf8, "UTF-8");

        AssertMalformed(CreateMetadata(1, true, false, "b", "a"), "ordered");
        AssertMalformed(CreateMetadata(1, true, false, "a", "a"), "unique");
    }

    [Fact]
    public void Rejects_dictionary_size_that_cannot_be_materialized() {
        byte[] raw = { 0xC1, 0xFF, 0xFF, 0xFF, 0xFF };

        AssertMalformed(raw, "too large");
    }

    private static void AssertMalformed(byte[] raw, string messagePart) {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => new VariantMetadata(raw));
        Assert.Contains(messagePart, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static byte[] CreateMetadata(int offsetSize, bool sortedStrings, bool reservedBit, params string[] values) {
        if(offsetSize is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(offsetSize));

        byte[][] encodedValues = new byte[values.Length][];
        int payloadLength = 0;
        for(int i = 0; i < values.Length; i++) {
            encodedValues[i] = Encoding.UTF8.GetBytes(values[i]);
            payloadLength = checked(payloadLength + encodedValues[i].Length);
        }

        int offsetCount = checked(values.Length + 1);
        int payloadStart = checked(1 + offsetSize + offsetCount * offsetSize);
        byte[] result = new byte[checked(payloadStart + payloadLength)];
        result[0] = (byte)(1 | (sortedStrings ? 0x10 : 0) | (reservedBit ? 0x20 : 0) | ((offsetSize - 1) << 6));
        WriteUnsigned(result, 1, offsetSize, (uint)values.Length);

        int offsetPosition = 1 + offsetSize;
        int payloadPosition = payloadStart;
        WriteUnsigned(result, offsetPosition, offsetSize, 0);
        offsetPosition += offsetSize;
        for(int i = 0; i < encodedValues.Length; i++) {
            Buffer.BlockCopy(encodedValues[i], 0, result, payloadPosition, encodedValues[i].Length);
            payloadPosition += encodedValues[i].Length;
            WriteUnsigned(result, offsetPosition, offsetSize, (uint)(payloadPosition - payloadStart));
            offsetPosition += offsetSize;
        }

        return result;
    }

    private static void WriteUnsigned(byte[] destination, int offset, int size, uint value) {
        for(int i = 0; i < size; i++) {
            destination[offset + i] = (byte)(value >> (i * 8));
        }
    }
}
