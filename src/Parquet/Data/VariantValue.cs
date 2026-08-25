using System;
using System.Text;

namespace Parquet.Data;

/// <summary>
/// The four basic types used by the Variant binary encoding.
/// </summary>
public enum VariantBasicType : byte {
    /// <summary>A primitive value.</summary>
    Primitive = 0,

    /// <summary>A UTF-8 string whose encoded length is less than 64 bytes.</summary>
    ShortString = 1,

    /// <summary>An object containing field names and Variant values.</summary>
    Object = 2,

    /// <summary>An ordered sequence of Variant values.</summary>
    Array = 3
}

/// <summary>
/// Primitive type identifiers used by the Variant binary encoding.
/// </summary>
public enum VariantPrimitiveType : byte {
    /// <summary>A null value.</summary>
    Null = 0,

    /// <summary>A true Boolean value.</summary>
    BooleanTrue = 1,

    /// <summary>A false Boolean value.</summary>
    BooleanFalse = 2,

    /// <summary>A signed 8-bit integer.</summary>
    Int8 = 3,

    /// <summary>A signed 16-bit integer.</summary>
    Int16 = 4,

    /// <summary>A signed 32-bit integer.</summary>
    Int32 = 5,

    /// <summary>A signed 64-bit integer.</summary>
    Int64 = 6,

    /// <summary>An IEEE 754 double-precision value.</summary>
    Double = 7,

    /// <summary>A decimal with a one-byte unscaled value.</summary>
    Decimal4 = 8,

    /// <summary>A decimal with an eight-byte unscaled value.</summary>
    Decimal8 = 9,

    /// <summary>A decimal with a sixteen-byte unscaled value.</summary>
    Decimal16 = 10,

    /// <summary>A date value.</summary>
    Date = 11,

    /// <summary>A UTC-adjusted timestamp with microsecond precision.</summary>
    TimestampMicros = 12,

    /// <summary>A timestamp without a time zone with microsecond precision.</summary>
    TimestampNtzMicros = 13,

    /// <summary>An IEEE 754 single-precision value.</summary>
    Float = 14,

    /// <summary>A length-prefixed binary value.</summary>
    Binary = 15,

    /// <summary>A length-prefixed UTF-8 string.</summary>
    String = 16,

    /// <summary>A time without a time zone with microsecond precision.</summary>
    TimeNtzMicros = 17,

    /// <summary>A UTC-adjusted timestamp with nanosecond precision.</summary>
    TimestampNanos = 18,

    /// <summary>A timestamp without a time zone with nanosecond precision.</summary>
    TimestampNtzNanos = 19,

    /// <summary>A 16-byte UUID.</summary>
    Uuid = 20
}

/// <summary>
/// A low-allocation view over one encoded Variant value.
/// </summary>
public readonly struct VariantValue {
    private const int MaxNestingDepth = 256;
    private readonly ReadOnlyMemory<byte> _rawData;
    private readonly VariantMetadata _metadata;
    private readonly int _elementCount;
    private readonly int _countSize;
    private readonly int _fieldIdTableOffset;
    private readonly int _fieldOffsetTableOffset;
    private readonly int _fieldsOffset;
    private readonly int _childDataLength;

    /// <summary>
    /// Constructs a view over an encoded Variant value.
    /// </summary>
    /// <param name="rawData">The complete binary encoding of one Variant value.</param>
    /// <param name="metadata">The metadata dictionary used by object values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="metadata"/> is null.</exception>
    /// <exception cref="ArgumentException">The value has an empty header or an unsupported primitive type.</exception>
    public VariantValue(ReadOnlyMemory<byte> rawData, VariantMetadata? metadata) {
        VariantMetadata resolvedMetadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        if(rawData.IsEmpty)
            throw new ArgumentException("Variant value must contain a value header.", nameof(rawData));

        byte valueMetadata = rawData.Span[0];
        VariantBasicType basicType = (VariantBasicType)(valueMetadata & 0x03);
        byte valueHeader = (byte)(valueMetadata >> 2);
        VariantPrimitiveType primitiveType = default;
        if(basicType == VariantBasicType.Primitive) {
            if(valueHeader > (byte)VariantPrimitiveType.Uuid)
                throw new ArgumentException($"Unsupported Variant primitive type: {valueHeader}.", nameof(rawData));

            primitiveType = (VariantPrimitiveType)valueHeader;
        }

        _rawData = rawData;
        _metadata = resolvedMetadata;
        BasicType = basicType;
        ValueHeader = valueHeader;
        ValueMetadata = valueMetadata;
        PrimitiveType = primitiveType;
        FieldOffsetSize = basicType is VariantBasicType.Object or VariantBasicType.Array
            ? (valueHeader & 0x03) + 1
            : 0;
        FieldIdSize = basicType == VariantBasicType.Object
            ? ((valueHeader >> 2) & 0x03) + 1
            : 0;
        IsLarge = basicType == VariantBasicType.Object
            ? (valueHeader & 0x10) != 0
            : basicType == VariantBasicType.Array && (valueHeader & 0x04) != 0;
        ShortStringLength = basicType == VariantBasicType.ShortString ? valueHeader : 0;

        int encodedLength = GetEncodedValueLength(rawData.Span, resolvedMetadata, 0, nameof(rawData), out CompositeInfo compositeInfo);
        if(encodedLength != rawData.Length)
            throw new ArgumentException("Variant value contains trailing bytes after the encoded value.", nameof(rawData));

        _elementCount = compositeInfo.ElementCount;
        _countSize = compositeInfo.CountSize;
        _fieldIdTableOffset = compositeInfo.FieldIdTableOffset;
        _fieldOffsetTableOffset = compositeInfo.FieldOffsetTableOffset;
        _fieldsOffset = compositeInfo.FieldsOffset;
        _childDataLength = compositeInfo.ChildDataLength;
    }

    /// <summary>
    /// Gets the complete encoded value, including its one-byte metadata header.
    /// </summary>
    public ReadOnlyMemory<byte> RawData => _rawData;

    /// <summary>
    /// Gets the encoded value data after the one-byte metadata header.
    /// </summary>
    public ReadOnlyMemory<byte> ValueData => _rawData[1..];

    /// <summary>
    /// Gets the basic Variant type.
    /// </summary>
    public VariantBasicType BasicType { get; }

    /// <summary>
    /// Gets the six-bit value header, excluding the two basic-type bits.
    /// </summary>
    public byte ValueHeader { get; }

    /// <summary>
    /// Gets the original one-byte value metadata header.
    /// </summary>
    public byte ValueMetadata { get; }

    /// <summary>
    /// Gets the primitive type identifier. This value is meaningful when <see cref="BasicType"/> is <see cref="VariantBasicType.Primitive"/>.
    /// </summary>
    public VariantPrimitiveType PrimitiveType { get; }

    /// <summary>
    /// Gets the primitive type identifier as encoded in the value header.
    /// </summary>
    public byte PrimitiveTypeId => ValueHeader;

    /// <summary>
    /// Gets the exact encoded payload of a primitive value.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not primitive.</exception>
    public ReadOnlyMemory<byte> PrimitivePayload => BasicType == VariantBasicType.Primitive
        ? ValueData
        : throw new InvalidOperationException("The Variant value is not primitive.");

    /// <summary>
    /// Gets the number of bytes used for object or array child offsets, or zero for other basic types.
    /// </summary>
    public int FieldOffsetSize { get; }

    /// <summary>
    /// Gets the number of bytes used for object field identifiers, or zero for other basic types.
    /// </summary>
    public int FieldIdSize { get; }

    /// <summary>
    /// Gets a value indicating whether an object or array uses a four-byte element count.
    /// </summary>
    public bool IsLarge { get; }

    /// <summary>
    /// Gets the short-string byte length, or zero for other basic types.
    /// </summary>
    public int ShortStringLength { get; }

    /// <summary>
    /// Gets the number of fields in an object or elements in an array.
    /// </summary>
    public int ElementCount => _elementCount;

    /// <summary>
    /// Gets the number of fields in this object.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an object.</exception>
    public int FieldCount => RequireComposite(VariantBasicType.Object);

    /// <summary>
    /// Gets the number of elements in this array.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an array.</exception>
    public int ArrayElementCount => RequireComposite(VariantBasicType.Array);

    /// <summary>
    /// Gets the number of bytes used to encode the object or array element count.
    /// </summary>
    public int ElementCountSize => _countSize;

    /// <summary>
    /// Gets the length of the encoded child-value area for an object or array.
    /// </summary>
    public int ChildDataLength => _childDataLength;

    /// <summary>
    /// Gets a raw encoded child value by its object field or array index.
    /// </summary>
    /// <param name="index">The zero-based child index.</param>
    /// <returns>A zero-copy slice of <see cref="RawData"/> containing the child value.</returns>
    public ReadOnlyMemory<byte> GetChildValue(int index) {
        EnsureChildIndex(index);
        ReadOnlySpan<byte> valueData = _rawData.Span[1..];
        uint offset = GetOffset(valueData, index);
        int childStart = checked(_fieldsOffset + (int)offset);
        int childLength;
        if(BasicType == VariantBasicType.Array) {
            uint nextOffset = GetOffset(valueData, index + 1);
            childLength = checked((int)(nextOffset - offset));
        }
        else {
            childLength = GetEncodedValueLength(valueData[childStart..], _metadata, 0, nameof(index), out _);
        }

        return _rawData.Slice(checked(1 + childStart), childLength);
    }

    /// <summary>
    /// Gets a nested child value view by its object field or array index.
    /// </summary>
    public VariantValue GetChild(int index) => new(GetChildValue(index), _metadata);

    /// <summary>
    /// Gets an object field identifier.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an object.</exception>
    public int GetFieldId(int index) {
        EnsureChildIndex(index, VariantBasicType.Object);
        return checked((int)ReadUnsignedLittleEndian(_rawData.Span[1..], _fieldIdTableOffset + index * FieldIdSize, FieldIdSize));
    }

    /// <summary>
    /// Gets an object field name resolved through <see cref="VariantMetadata"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an object.</exception>
    public string GetFieldName(int index) => _metadata.Dictionary[GetFieldId(index)];

    /// <summary>
    /// Gets an object field value as a raw memory slice.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an object.</exception>
    public ReadOnlyMemory<byte> GetFieldValue(int index) {
        EnsureChildIndex(index, VariantBasicType.Object);
        return GetChildValue(index);
    }

    /// <summary>
    /// Gets an object field value as a nested view.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an object.</exception>
    public VariantValue GetField(int index) => new(GetFieldValue(index), _metadata);

    /// <summary>
    /// Gets an array element as a raw memory slice.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an array.</exception>
    public ReadOnlyMemory<byte> GetArrayElement(int index) {
        EnsureChildIndex(index, VariantBasicType.Array);
        return GetChildValue(index);
    }

    /// <summary>
    /// Gets an array element as a nested view.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value is not an array.</exception>
    public VariantValue GetArrayElementValue(int index) => new(GetArrayElement(index), _metadata);

    private int RequireComposite(VariantBasicType expectedType) {
        if(BasicType != expectedType)
            throw new InvalidOperationException($"The Variant value is not an {expectedType.ToString().ToLowerInvariant()}.");

        return _elementCount;
    }

    private void EnsureChildIndex(int index) {
        if(BasicType is not (VariantBasicType.Object or VariantBasicType.Array))
            throw new InvalidOperationException("The Variant value does not contain child values.");

        EnsureChildIndex(index, BasicType);
    }

    private void EnsureChildIndex(int index, VariantBasicType expectedType) {
        RequireComposite(expectedType);
        if((uint)index >= (uint)_elementCount)
            throw new ArgumentOutOfRangeException(nameof(index));
    }

    private uint GetOffset(ReadOnlySpan<byte> valueData, int index) {
        return ReadUnsignedLittleEndian(valueData, checked(_fieldOffsetTableOffset + index * FieldOffsetSize), FieldOffsetSize);
    }

    private static uint ReadUnsignedLittleEndian(ReadOnlySpan<byte> source, int offset, int size) {
        uint value = 0;
        for(int i = 0; i < size; i++) {
            value |= (uint)source[offset + i] << (8 * i);
        }

        return value;
    }

    private static int GetEncodedValueLength(ReadOnlySpan<byte> encodedValue, VariantMetadata metadata, int depth, string parameterName, out CompositeInfo compositeInfo) {
        compositeInfo = default;
        if(depth > MaxNestingDepth)
            throw new ArgumentException("Variant value nesting depth is too large.", parameterName);
        if(encodedValue.IsEmpty)
            throw new ArgumentException("Variant child value is truncated before its header.", parameterName);

        byte valueMetadata = encodedValue[0];
        VariantBasicType basicType = (VariantBasicType)(valueMetadata & 0x03);
        byte valueHeader = (byte)(valueMetadata >> 2);
        switch(basicType) {
            case VariantBasicType.Primitive:
                return checked(1 + GetPrimitiveDataLength(encodedValue[1..], (VariantPrimitiveType)valueHeader, parameterName));
            case VariantBasicType.ShortString:
                EnsureAvailableLength(encodedValue[1..], valueHeader, "short string", parameterName);
                ValidateUtf8(encodedValue.Slice(1, valueHeader), "short string", parameterName);
                return checked(1 + valueHeader);
            case VariantBasicType.Object:
            case VariantBasicType.Array:
                int compositeLength = ParseComposite(encodedValue[1..], basicType, valueHeader, metadata, depth, parameterName, out compositeInfo);
                return checked(1 + compositeLength);
            default:
                throw new ArgumentException($"Unsupported Variant basic type: {(byte)basicType}.", parameterName);
        }
    }

    private static int GetPrimitiveDataLength(ReadOnlySpan<byte> valueData, VariantPrimitiveType primitiveType, string parameterName) {
        int expectedLength = primitiveType switch {
            VariantPrimitiveType.Null or VariantPrimitiveType.BooleanTrue or VariantPrimitiveType.BooleanFalse => 0,
            VariantPrimitiveType.Int8 => 1,
            VariantPrimitiveType.Int16 => 2,
            VariantPrimitiveType.Int32 => 4,
            VariantPrimitiveType.Int64 or VariantPrimitiveType.Double or VariantPrimitiveType.TimestampMicros
                or VariantPrimitiveType.TimestampNtzMicros or VariantPrimitiveType.TimeNtzMicros
                or VariantPrimitiveType.TimestampNanos or VariantPrimitiveType.TimestampNtzNanos => 8,
            VariantPrimitiveType.Decimal4 => 5,
            VariantPrimitiveType.Decimal8 => 9,
            VariantPrimitiveType.Decimal16 => 17,
            VariantPrimitiveType.Date or VariantPrimitiveType.Float => 4,
            VariantPrimitiveType.Uuid => 16,
            VariantPrimitiveType.Binary or VariantPrimitiveType.String => -1,
            _ => throw new ArgumentException($"Unsupported Variant primitive type: {(byte)primitiveType}.", parameterName)
        };

        if(expectedLength >= 0) {
            EnsureAvailableLength(valueData, expectedLength, $"primitive type {(byte)primitiveType}", parameterName);
            if(primitiveType is VariantPrimitiveType.Decimal4 or VariantPrimitiveType.Decimal8 or VariantPrimitiveType.Decimal16) {
                if(valueData[0] > 38)
                    throw new ArgumentException("Variant decimal scale must be in the range 0 through 38.", parameterName);
            }

            return expectedLength;
        }

        if(valueData.Length < sizeof(uint))
            throw new ArgumentException("Variant length-prefixed primitive is truncated before its length.", parameterName);

        uint payloadLength = ReadUnsignedLittleEndian(valueData, 0, sizeof(uint));
        if(payloadLength > (uint)(valueData.Length - sizeof(uint)))
            throw new ArgumentException("Variant length-prefixed primitive length exceeds the available payload.", parameterName);

        int expectedTotalLength = checked(sizeof(uint) + (int)payloadLength);
        EnsureAvailableLength(valueData, expectedTotalLength, "length-prefixed primitive", parameterName);
        if(primitiveType == VariantPrimitiveType.String)
            ValidateUtf8(valueData.Slice(sizeof(uint), (int)payloadLength), "string", parameterName);

        return expectedTotalLength;
    }

    private static void EnsureAvailableLength(ReadOnlySpan<byte> valueData, int requiredLength, string valueKind, string parameterName) {
        if(valueData.Length < requiredLength)
            throw new ArgumentException($"Variant {valueKind} payload is truncated; it requires {requiredLength} bytes.", parameterName);
    }

    private static void ValidateUtf8(ReadOnlySpan<byte> valueData, string valueKind, string parameterName) {
        try {
            UTF8Encoding encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
            _ = encoding.GetCharCount(valueData);
        }
        catch(DecoderFallbackException exception) {
            throw new ArgumentException($"Variant {valueKind} payload is not valid UTF-8.", parameterName, exception);
        }
    }

    private static int ParseComposite(ReadOnlySpan<byte> valueData, VariantBasicType basicType, byte valueHeader, VariantMetadata metadata,
        int depth, string parameterName, out CompositeInfo info) {
        int countSize = (basicType == VariantBasicType.Object ? (valueHeader & 0x10) : (valueHeader & 0x04)) != 0 ? 4 : 1;
        int fieldOffsetSize = (valueHeader & 0x03) + 1;
        int fieldIdSize = basicType == VariantBasicType.Object ? ((valueHeader >> 2) & 0x03) + 1 : 0;

        EnsureAvailableLength(valueData, countSize, "composite element count", parameterName);
        uint elementCountValue = ReadUnsignedLittleEndian(valueData, 0, countSize);
        if(elementCountValue > int.MaxValue)
            throw new ArgumentException("Variant composite element count is too large.", parameterName);

        int elementCount = (int)elementCountValue;
        ulong fieldIdBytes = (ulong)elementCount * (uint)fieldIdSize;
        ulong offsetCount = (ulong)elementCount + 1;
        ulong fieldOffsetBytes = offsetCount * (uint)fieldOffsetSize;
        ulong fieldIdTableOffset = (ulong)countSize;
        ulong fieldOffsetTableOffset = fieldIdTableOffset + fieldIdBytes;
        ulong fieldsOffset = fieldOffsetTableOffset + fieldOffsetBytes;
        if(fieldsOffset > (ulong)valueData.Length)
            throw new ArgumentException("Variant composite value is truncated in its field table.", parameterName);

        int fieldIdTableStart = checked((int)fieldIdTableOffset);
        int fieldOffsetTableStart = checked((int)fieldOffsetTableOffset);
        int fieldsStart = checked((int)fieldsOffset);
        ValidateFieldIds(basicType, valueData, fieldIdTableStart, elementCount, fieldIdSize, metadata, parameterName);

        uint terminalOffset = ReadUnsignedLittleEndian(valueData, checked(fieldOffsetTableStart + elementCount * fieldOffsetSize), fieldOffsetSize);
        if(terminalOffset > (uint)(valueData.Length - fieldsStart))
            throw new ArgumentException("Variant composite terminal offset exceeds its child-value data.", parameterName);

        int childDataLength = checked((int)terminalOffset);
        ulong childLengthSum = 0;
        uint previousOffset = 0;
        for(int i = 0; i < elementCount; i++) {
            uint offset = ReadUnsignedLittleEndian(valueData, fieldOffsetTableStart + i * fieldOffsetSize, fieldOffsetSize);
            if(offset > terminalOffset)
                throw new ArgumentException("Variant composite child offset exceeds the terminal offset.", parameterName);

            if(basicType == VariantBasicType.Array && offset < previousOffset)
                throw new ArgumentException("Variant array child offsets must be nondecreasing.", parameterName);

            uint nextOffset = ReadUnsignedLittleEndian(valueData, fieldOffsetTableStart + (i + 1) * fieldOffsetSize, fieldOffsetSize);
            if(basicType == VariantBasicType.Array && nextOffset < offset)
                throw new ArgumentException("Variant array child offsets must be nondecreasing.", parameterName);

            int availableLength = checked(childDataLength - (int)offset);
            int childLength = GetEncodedValueLength(valueData.Slice(fieldsStart + (int)offset, availableLength), metadata, depth + 1, parameterName, out _);
            if(basicType == VariantBasicType.Array && childLength != checked((int)(nextOffset - offset)))
                throw new ArgumentException("Variant array offsets do not match the encoded child lengths.", parameterName);

            childLengthSum = checked(childLengthSum + (uint)childLength);
            previousOffset = offset;
        }

        if(childLengthSum != terminalOffset)
            throw new ArgumentException("Variant composite child offsets do not cover the complete child-value data.", parameterName);

        info = new CompositeInfo(elementCount, countSize, fieldIdTableStart, fieldOffsetTableStart, fieldsStart, childDataLength);
        return checked(fieldsStart + childDataLength);
    }

    private static void ValidateFieldIds(VariantBasicType basicType, ReadOnlySpan<byte> valueData, int tableStart, int elementCount,
        int fieldIdSize, VariantMetadata metadata, string parameterName) {
        if(basicType != VariantBasicType.Object)
            return;

        string? previousName = null;
        for(int i = 0; i < elementCount; i++) {
            uint fieldId = ReadUnsignedLittleEndian(valueData, checked(tableStart + i * fieldIdSize), fieldIdSize);
            if(fieldId >= (uint)metadata.Dictionary.Count)
                throw new ArgumentException("Variant object field ID does not reference the metadata dictionary.", parameterName);

            string fieldName = metadata.Dictionary[(int)fieldId];
            if(previousName is not null && CompareUtf8(previousName, fieldName) >= 0)
                throw new ArgumentException("Variant object field names must be unique and ordered by UTF-8 bytes.", parameterName);

            previousName = fieldName;
        }
    }

    private static int CompareUtf8(string left, string right) {
        byte[] leftBytes = Encoding.UTF8.GetBytes(left);
        byte[] rightBytes = Encoding.UTF8.GetBytes(right);
        int length = Math.Min(leftBytes.Length, rightBytes.Length);
        for(int i = 0; i < length; i++) {
            int comparison = leftBytes[i].CompareTo(rightBytes[i]);
            if(comparison != 0)
                return comparison;
        }

        return leftBytes.Length.CompareTo(rightBytes.Length);
    }

    private readonly struct CompositeInfo {
        public CompositeInfo(int elementCount, int countSize, int fieldIdTableOffset, int fieldOffsetTableOffset, int fieldsOffset, int childDataLength) {
            ElementCount = elementCount;
            CountSize = countSize;
            FieldIdTableOffset = fieldIdTableOffset;
            FieldOffsetTableOffset = fieldOffsetTableOffset;
            FieldsOffset = fieldsOffset;
            ChildDataLength = childDataLength;
        }

        public int ElementCount { get; }
        public int CountSize { get; }
        public int FieldIdTableOffset { get; }
        public int FieldOffsetTableOffset { get; }
        public int FieldsOffset { get; }
        public int ChildDataLength { get; }
    }
}
