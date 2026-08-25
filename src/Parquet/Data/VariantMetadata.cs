using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data;

/// <summary>
/// Variant data type metadata wrapper to encode/decode individual variant metadata value.
/// </summary>
public readonly struct VariantMetadata {
    
    private static readonly UTF8Encoding _utf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
    
    /// <summary>
    /// Gets the metadata encoding version. Only used for informational purposes, you don't need to use it.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// Gets a value indicating whether the dictionary strings are sorted and unique.
    /// </summary>
    public bool DictionaryIsSortedAndUnique { get; }

    /// <summary>
    /// Gets the dictionary strings in dictionary/index order.
    /// </summary>
    public IReadOnlyList<string> Dictionary { get; }

    /// <summary>
    /// Constructs metadata from its binary encoding as specified in the Parquet format specification.
    /// See https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#metadata-encoding
    /// </summary>
    /// <param name="rawData">The binary Variant metadata encoding.</param>
    /// <exception cref="ArgumentException">The encoding is malformed or uses an unsupported version.</exception>
    public VariantMetadata(ReadOnlyMemory<byte> rawData) {
        ReadOnlySpan<byte> raw = rawData.Span;
        if(raw.IsEmpty)
            throw new ArgumentException("Variant metadata must contain a header.", nameof(rawData));

        byte header = raw[0];
        int version = header & 0x0F;
        if(version != 1)
            throw new ArgumentException($"Only version 1 (detected {version}) is supported.", nameof(rawData));

        DictionaryIsSortedAndUnique = (header & 0x10) != 0;
        int offsetSize = ((header >> 6) & 0x03) + 1;
        if(raw.Length < 1 + offsetSize)
            throw new ArgumentException("Variant metadata is truncated before the dictionary size.", nameof(rawData));

        uint dictionarySizeValue = ReadUnsignedLittleEndian(raw, 1, offsetSize);
        if(dictionarySizeValue > int.MaxValue)
            throw new ArgumentException("Variant metadata dictionary size is too large.", nameof(rawData));

        int dictionarySize = (int)dictionarySizeValue;
        ulong offsetCount = (ulong)dictionarySize + 1;
        ulong offsetTableBytes = offsetCount * (uint)offsetSize;
        ulong offsetTableStart = (ulong)(1 + offsetSize);
        ulong payloadStart = offsetTableStart + offsetTableBytes;
        if(payloadStart > (ulong)raw.Length)
            throw new ArgumentException("Variant metadata is truncated in the dictionary offset table.", nameof(rawData));

        ReadOnlySpan<byte> payload = raw[(int)payloadStart..];
        uint previousOffset = 0;
        var dictionary = new List<string>(dictionarySize);

        for(int i = 0; i <= dictionarySize; i++) {
            ulong offsetPosition = offsetTableStart + ((ulong)i * (uint)offsetSize);
            uint currentOffset = ReadUnsignedLittleEndian(raw, (int)offsetPosition, offsetSize);
            if(i == 0 && currentOffset != 0)
                throw new ArgumentException("The first Variant metadata dictionary offset must be zero.", nameof(rawData));

            if(currentOffset < previousOffset)
                throw new ArgumentException("Variant metadata dictionary offsets must be nondecreasing.", nameof(rawData));

            if(currentOffset > (uint)payload.Length)
                throw new ArgumentException("A Variant metadata dictionary offset exceeds the dictionary payload.", nameof(rawData));

            if(i > 0) {
                int entryStart = checked((int)previousOffset);
                int entryLength = checked((int)(currentOffset - previousOffset));
                ReadOnlySpan<byte> entry = payload.Slice(entryStart, entryLength);
                string value;
                try {
                    value = _utf8.GetString(entry);
                }
                catch(DecoderFallbackException exception) {
                    throw new ArgumentException("A Variant metadata dictionary entry is not valid UTF-8.", nameof(rawData), exception);
                }

                dictionary.Add(value);
            }

            previousOffset = currentOffset;
        }

        if(previousOffset != (uint)payload.Length)
            throw new ArgumentException("The final Variant metadata dictionary offset must match the payload length.", nameof(rawData));

        Version = version;
        Dictionary = Array.AsReadOnly([.. dictionary]);
    }

    private static uint ReadUnsignedLittleEndian(ReadOnlySpan<byte> source, int offset, int size) {
        uint value = 0;
        for(int i = 0; i < size; i++) {
            value |= (uint)source[offset + i] << (i * 8);
        }

        return value;
    }

    /// <inheritdoc />
    public override string ToString() => $"v{Version}, sorted: {DictionaryIsSortedAndUnique}, count: {Dictionary.Count}";
}