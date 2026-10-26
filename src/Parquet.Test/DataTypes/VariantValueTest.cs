using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Xunit;

namespace Parquet.Test.DataTypes;

public class VariantValueTest {
    private static readonly VariantMetadata EmptyMetadata = new(new byte[] { 0x01, 0x00, 0x00 });

    [Fact]
    public void Rejects_empty_value() {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => new VariantValue(ReadOnlyMemory<byte>.Empty, EmptyMetadata));

        Assert.Contains("header", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Rejects_null_metadata() {
        Assert.Throws<ArgumentNullException>(() => new VariantValue(new byte[] { 0x00 }, null!));
    }

    [Theory]
    [InlineData(VariantBasicType.Primitive)]
    [InlineData(VariantBasicType.ShortString)]
    [InlineData(VariantBasicType.Object)]
    [InlineData(VariantBasicType.Array)]
    public void Classifies_basic_type_and_preserves_header(VariantBasicType expectedBasicType) {
        byte valueHeader = expectedBasicType == VariantBasicType.Primitive ? (byte)20 : (byte)0;
        byte header = (byte)((valueHeader << 2) | (byte)expectedBasicType);
        byte[] raw = new byte[expectedBasicType == VariantBasicType.Primitive ? 17 : expectedBasicType == VariantBasicType.ShortString ? 1 : 3];
        raw[0] = header;
        var value = new VariantValue(raw, EmptyMetadata);

        Assert.Equal(expectedBasicType, value.BasicType);
        Assert.Equal((byte)(header >> 2), value.ValueHeader);
        Assert.Equal(header, value.ValueMetadata);
        Assert.Equal(raw[1..], value.ValueData.ToArray());
        Assert.Equal(header, value.RawData.Span[0]);
    }

    [Theory]
    [InlineData(0, VariantPrimitiveType.Null)]
    [InlineData(1, VariantPrimitiveType.BooleanTrue)]
    [InlineData(2, VariantPrimitiveType.BooleanFalse)]
    [InlineData(3, VariantPrimitiveType.Int8)]
    [InlineData(4, VariantPrimitiveType.Int16)]
    [InlineData(5, VariantPrimitiveType.Int32)]
    [InlineData(6, VariantPrimitiveType.Int64)]
    [InlineData(7, VariantPrimitiveType.Double)]
    [InlineData(8, VariantPrimitiveType.Decimal4)]
    [InlineData(9, VariantPrimitiveType.Decimal8)]
    [InlineData(10, VariantPrimitiveType.Decimal16)]
    [InlineData(11, VariantPrimitiveType.Date)]
    [InlineData(12, VariantPrimitiveType.TimestampMicros)]
    [InlineData(13, VariantPrimitiveType.TimestampNtzMicros)]
    [InlineData(14, VariantPrimitiveType.Float)]
    [InlineData(15, VariantPrimitiveType.Binary)]
    [InlineData(16, VariantPrimitiveType.String)]
    [InlineData(17, VariantPrimitiveType.TimeNtzMicros)]
    [InlineData(18, VariantPrimitiveType.TimestampNanos)]
    [InlineData(19, VariantPrimitiveType.TimestampNtzNanos)]
    [InlineData(20, VariantPrimitiveType.Uuid)]
    public void Recognizes_supported_primitive_type_ids(int primitiveId, VariantPrimitiveType expectedType) {
        var value = new VariantValue(CreatePrimitiveValue(primitiveId), EmptyMetadata);

        Assert.Equal(VariantBasicType.Primitive, value.BasicType);
        Assert.Equal((byte)primitiveId, value.ValueHeader);
        Assert.Equal(expectedType, value.PrimitiveType);
    }

    [Fact]
    public void Rejects_unsupported_primitive_type_id() {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => new VariantValue(new byte[] { (byte)(21 << 2) }, EmptyMetadata));

        Assert.Contains("primitive", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Parses_short_string_length() {
        var value = new VariantValue(CreateShortString(new string('x', 63)), EmptyMetadata);

        Assert.Equal(VariantBasicType.ShortString, value.BasicType);
        Assert.Equal(63, value.ShortStringLength);
        Assert.Equal(0, value.FieldOffsetSize);
        Assert.Equal(0, value.FieldIdSize);
        Assert.False(value.IsLarge);
    }

    [Fact]
    public void Preserves_primitive_payload_without_conversion() {
        byte[] raw = CreatePrimitiveValue(6);
        raw[1] = 0x80;
        raw[8] = 0x7F;
        var value = new VariantValue(raw, EmptyMetadata);

        Assert.Equal(raw[1..], value.PrimitivePayload.ToArray());
        Assert.Equal((byte)6, value.PrimitiveTypeId);
    }

    [Fact]
    public void Validates_short_string_utf8_and_boundaries() {
        Assert.Equal(new byte[] { 0xC3, 0xA9 }, new VariantValue(CreateShortString("é"), EmptyMetadata).ValueData.ToArray());
        AssertMalformed(new byte[] { (byte)((2 << 2) | 1), 0xC3, 0x28 }, "UTF-8");
        AssertMalformed(new byte[] { (byte)((2 << 2) | 1), 0x41 }, "truncated");
        AssertMalformed(new byte[] { (byte)((1 << 2) | 1), 0x41, 0x42 }, "trailing");
    }

    [Fact]
    public void Validates_fixed_primitive_payload_sizes_and_decimal_scale() {
        byte[] truncated = { (byte)(6 << 2), 0, 0, 0, 0, 0, 0, 0 };
        AssertMalformed(truncated, "truncated");

        byte[] decimalData = CreatePrimitiveValue(8);
        decimalData[1] = 39;
        AssertMalformed(decimalData, "scale");

        byte[] noData = { 0x00, 0x01 };
        AssertMalformed(noData, "trailing");
    }

    [Fact]
    public void Validates_length_prefixed_binary_and_string_payloads() {
        byte[] binary = { (byte)(15 << 2), 2, 0, 0, 0, 0x01, 0x02 };
        Assert.Equal(new byte[] { 0x01, 0x02 }, new VariantValue(binary, EmptyMetadata).ValueData.Slice(4).ToArray());

        byte[] text = { (byte)(16 << 2), 2, 0, 0, 0, 0xC3, 0xA9 };
        Assert.Equal(7, new VariantValue(text, EmptyMetadata).RawData.Length);

        AssertMalformed(new byte[] { (byte)(15 << 2), 3, 0, 0, 0, 0x01 }, "exceeds");
        AssertMalformed(new byte[] { (byte)(16 << 2), 1, 0, 0, 0, 0xFF }, "UTF-8");
        AssertMalformed(new byte[] { (byte)(15 << 2), 0, 0, 0, 0, 0x01 }, "trailing");
    }

    [Fact]
    public void Parses_object_header_widths_and_reserved_bit() {
        const byte valueHeader = 0x1F; // reserved bit, large count, 4-byte field IDs, 4-byte offsets
        byte[] raw = { (byte)((valueHeader << 2) | 2), 0, 0, 0, 0, 0, 0, 0, 0 };
        var value = new VariantValue(raw, EmptyMetadata);

        Assert.Equal(VariantBasicType.Object, value.BasicType);
        Assert.Equal(4, value.FieldOffsetSize);
        Assert.Equal(4, value.FieldIdSize);
        Assert.True(value.IsLarge);
    }

    [Fact]
    public void Parses_array_header_widths_and_reserved_bits() {
        const byte valueHeader = 0x27; // reserved bits, large count, 4-byte offsets
        byte[] raw = { (byte)((valueHeader << 2) | 3), 0, 0, 0, 0, 0, 0, 0, 0 };
        var value = new VariantValue(raw, EmptyMetadata);

        Assert.Equal(VariantBasicType.Array, value.BasicType);
        Assert.Equal(4, value.FieldOffsetSize);
        Assert.Equal(0, value.FieldIdSize);
        Assert.True(value.IsLarge);
    }

    [Fact]
    public void Parses_object_fields_with_nonmonotonic_offsets() {
        VariantMetadata metadata = CreateMetadata("a", "b", "c");
        byte[][] physicalValues = { CreateInt8Value(30), CreateInt8Value(20), CreateInt8Value(10) };
        byte[] raw = CreateObject(new[] { 0, 1, 2 }, physicalValues, new uint[] { 4, 2, 0, 6 }, 1, 1, false);
        var value = new VariantValue(raw, metadata);

        Assert.Equal(3, value.FieldCount);
        Assert.Equal(new[] { 0, 1, 2 }, new[] { value.GetFieldId(0), value.GetFieldId(1), value.GetFieldId(2) });
        Assert.Equal(new[] { "a", "b", "c" }, new[] { value.GetFieldName(0), value.GetFieldName(1), value.GetFieldName(2) });
        Assert.Equal((byte)10, value.GetField(0).PrimitivePayload.Span[0]);
        Assert.Equal((byte)20, value.GetField(1).PrimitivePayload.Span[0]);
        Assert.Equal((byte)30, value.GetField(2).PrimitivePayload.Span[0]);
    }

    [Fact]
    public void Orders_object_fields_by_utf8_bytes() {
        VariantMetadata metadata = CreateMetadata("\uE000", "😀");
        byte[] raw = CreateObject(new[] { 0, 1 }, new[] { CreateInt8Value(2), CreateInt8Value(1) }, new uint[] { 2, 0, 4 }, 1, 1, false);
        var value = new VariantValue(raw, metadata);

        Assert.Equal("\uE000", value.GetFieldName(0));
        Assert.Equal("😀", value.GetFieldName(1));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Parses_object_widths_and_large_count(int width) {
        VariantMetadata metadata = CreateMetadata("field");
        byte[] raw = CreateObject(new[] { 0 }, new[] { CreateInt8Value(7) }, new uint[] { 0, 2 }, width, width, true);
        var value = new VariantValue(raw, metadata);

        Assert.Equal(1, value.ElementCount);
        Assert.Equal(4, value.ElementCountSize);
        Assert.Equal(width, value.FieldOffsetSize);
        Assert.Equal(width, value.FieldIdSize);
        Assert.Equal((byte)7, value.GetFieldValue(0).Span[1]);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Parses_array_widths_and_nested_values(int width) {
        byte[] raw = CreateArray(new[] { CreateInt8Value(9), CreateShortString("ok") }, width, false);
        var value = new VariantValue(raw, EmptyMetadata);

        Assert.Equal(2, value.ArrayElementCount);
        Assert.Equal(width, value.FieldOffsetSize);
        Assert.Equal((byte)9, value.GetArrayElementValue(0).PrimitivePayload.Span[0]);
        Assert.Equal("ok", Encoding.UTF8.GetString(value.GetArrayElement(1).Span[1..]));
    }

    [Fact]
    public void Parses_large_array_and_nested_object() {
        VariantMetadata metadata = CreateMetadata("a");
        byte[] nestedObject = CreateObject(new[] { 0 }, new[] { CreateInt8Value(4) }, new uint[] { 0, 2 }, 1, 1, false);
        byte[] raw = CreateArray(new[] { nestedObject }, 1, true);
        var value = new VariantValue(raw, metadata);

        Assert.True(value.IsLarge);
        Assert.Equal(4, value.ElementCountSize);
        Assert.Equal(4, value.GetArrayElementValue(0).GetField(0).PrimitivePayload.Span[0]);
    }

    [Fact]
    public void Rejects_invalid_composite_tables_and_children() {
        AssertMalformed(new byte[] { 0x02, 1, 1, 0, 0 }, "dictionary");

        byte[] invalidArray = CreateArray(new[] { CreateInt8Value(1) }, 1, false);
        invalidArray[3] = 3;
        AssertMalformed(invalidArray, "terminal");

        byte[] truncatedChild = CreateArray(new[] { CreateInt8Value(1) }, 1, false);
        Array.Resize(ref truncatedChild, truncatedChild.Length - 1);
        AssertMalformed(truncatedChild, "child");
    }

    private static byte[] CreatePrimitiveValue(int primitiveId) {
        int payloadLength = primitiveId switch {
            0 or 1 or 2 => 0,
            3 => 1,
            4 => 2,
            5 or 11 or 14 => 4,
            6 or 7 or 12 or 13 or 17 or 18 or 19 => 8,
            8 => 5,
            9 => 9,
            10 => 17,
            15 or 16 => 4,
            20 => 16,
            _ => throw new ArgumentOutOfRangeException(nameof(primitiveId))
        };

        byte[] result = new byte[payloadLength + 1];
        result[0] = (byte)(primitiveId << 2);
        return result;
    }

    private static byte[] CreateInt8Value(byte value) {
        return new[] { (byte)(3 << 2), value };
    }

    private static VariantMetadata CreateMetadata(params string[] names) {
        List<byte> payload = new();
        int[] offsets = new int[names.Length + 1];
        for(int i = 0; i < names.Length; i++) {
            byte[] encoded = Encoding.UTF8.GetBytes(names[i]);
            payload.AddRange(encoded);
            offsets[i + 1] = payload.Count;
        }

        byte[] raw = new byte[1 + 1 + offsets.Length + payload.Count];
        raw[0] = 1;
        raw[1] = (byte)names.Length;
        for(int i = 0; i < offsets.Length; i++)
            raw[2 + i] = (byte)offsets[i];
        payload.CopyTo(raw, 2 + offsets.Length);
        return new VariantMetadata(raw);
    }

    private static byte[] CreateObject(int[] fieldIds, byte[][] physicalValues, uint[] offsets, int fieldIdSize, int fieldOffsetSize, bool large) {
        int countSize = large ? 4 : 1;
        int fieldsLength = 0;
        foreach(byte[] physicalValue in physicalValues)
            fieldsLength = checked(fieldsLength + physicalValue.Length);

        int fieldIdsOffset = countSize;
        int fieldOffsetsOffset = checked(fieldIdsOffset + fieldIds.Length * fieldIdSize);
        int fieldsOffset = checked(fieldOffsetsOffset + offsets.Length * fieldOffsetSize);
        byte[] raw = new byte[checked(1 + fieldsOffset + fieldsLength)];
        byte valueHeader = (byte)((fieldOffsetSize - 1) | ((fieldIdSize - 1) << 2) | (large ? 0x10 : 0));
        raw[0] = (byte)((valueHeader << 2) | 2);
        WriteUnsigned(raw, 1, countSize, (uint)fieldIds.Length);
        for(int i = 0; i < fieldIds.Length; i++)
            WriteUnsigned(raw, 1 + fieldIdsOffset + i * fieldIdSize, fieldIdSize, (uint)fieldIds[i]);
        for(int i = 0; i < offsets.Length; i++)
            WriteUnsigned(raw, 1 + fieldOffsetsOffset + i * fieldOffsetSize, fieldOffsetSize, offsets[i]);

        int position = 1 + fieldsOffset;
        foreach(byte[] physicalValue in physicalValues) {
            physicalValue.CopyTo(raw, position);
            position += physicalValue.Length;
        }

        return raw;
    }

    private static byte[] CreateArray(byte[][] values, int fieldOffsetSize, bool large) {
        int countSize = large ? 4 : 1;
        uint[] offsets = new uint[values.Length + 1];
        for(int i = 0; i < values.Length; i++)
            offsets[i + 1] = checked(offsets[i] + (uint)values[i].Length);

        int fieldsOffset = checked(countSize + offsets.Length * fieldOffsetSize);
        byte[] raw = new byte[checked(1 + fieldsOffset + (int)offsets[^1])];
        byte valueHeader = (byte)((fieldOffsetSize - 1) | (large ? 0x04 : 0));
        raw[0] = (byte)((valueHeader << 2) | 3);
        WriteUnsigned(raw, 1, countSize, (uint)values.Length);
        for(int i = 0; i < offsets.Length; i++)
            WriteUnsigned(raw, 1 + countSize + i * fieldOffsetSize, fieldOffsetSize, offsets[i]);

        int position = 1 + fieldsOffset;
        foreach(byte[] value in values) {
            value.CopyTo(raw, position);
            position += value.Length;
        }

        return raw;
    }

    private static void WriteUnsigned(byte[] destination, int offset, int size, uint value) {
        for(int i = 0; i < size; i++)
            destination[offset + i] = (byte)(value >> (8 * i));
    }

    private static byte[] CreateShortString(string value) {
        byte[] encoded = System.Text.Encoding.UTF8.GetBytes(value);
        byte[] raw = new byte[encoded.Length + 1];
        raw[0] = (byte)((encoded.Length << 2) | 1);
        encoded.CopyTo(raw, 1);
        return raw;
    }

    private static void AssertMalformed(byte[] raw, string messagePart) {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => new VariantValue(raw, EmptyMetadata));
        Assert.Contains(messagePart, exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
