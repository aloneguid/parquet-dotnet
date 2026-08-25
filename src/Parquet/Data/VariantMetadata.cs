using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data;

/// <summary>
/// Variant data type metadata wrapper.
/// </summary>
public readonly struct VariantMetadata {
    private readonly IReadOnlyList<string> _dictionary;

    /// <summary>
    /// Gets the metadata encoding version.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// Gets a value indicating whether the dictionary strings are sorted.
    /// </summary>
    public bool SortedStrings { get; }

    /// <summary>
    /// Gets a value indicating whether the dictionary strings are sorted.
    /// </summary>
    public bool IsSortedStrings => SortedStrings;

    /// <summary>
    /// Gets the number of bytes used to encode dictionary offsets.
    /// </summary>
    public int OffsetSize { get; }

    /// <summary>
    /// Gets the number of strings in the dictionary.
    /// </summary>
    public int DictionarySize { get; }

    /// <summary>
    /// Gets the dictionary strings in dictionary/index order.
    /// </summary>
    public IReadOnlyList<string> Dictionary => _dictionary;

    /// <summary>
    /// Gets a dictionary string by its index.
    /// </summary>
    /// <param name="index">The dictionary index.</param>
    /// <returns>The dictionary string at <paramref name="index"/>.</returns>
    public string this[int index] => _dictionary[index];

    /// <summary>
    /// Constructs metadata from its binary encoding.
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
            throw new ArgumentException($"Unsupported Variant metadata version: {version}.", nameof(rawData));

        bool sortedStrings = (header & 0x10) != 0;
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
        ReadOnlySpan<byte> previousEntry = default;
        var dictionary = new List<string>(dictionarySize);
        UTF8Encoding utf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

        for(int i = 0; i <= dictionarySize; i++) {
            ulong offsetPosition = offsetTableStart + (ulong)i * (uint)offsetSize;
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
                    value = utf8.GetString(entry);
                }
                catch(DecoderFallbackException exception) {
                    throw new ArgumentException("A Variant metadata dictionary entry is not valid UTF-8.", nameof(rawData), exception);
                }

                if(sortedStrings && i > 1 && CompareUnsigned(previousEntry, entry) >= 0)
                    throw new ArgumentException("Sorted Variant metadata dictionary entries must be unique and ordered by UTF-8 bytes.", nameof(rawData));

                dictionary.Add(value);
                previousEntry = entry;
            }

            previousOffset = currentOffset;
        }

        if(previousOffset != (uint)payload.Length)
            throw new ArgumentException("The final Variant metadata dictionary offset must match the payload length.", nameof(rawData));

        Version = version;
        SortedStrings = sortedStrings;
        OffsetSize = offsetSize;
        DictionarySize = dictionarySize;
        _dictionary = Array.AsReadOnly(dictionary.ToArray());
    }

    private static uint ReadUnsignedLittleEndian(ReadOnlySpan<byte> source, int offset, int size) {
        uint value = 0;
        for(int i = 0; i < size; i++) {
            value |= (uint)source[offset + i] << (i * 8);
        }

        return value;
    }

    private static int CompareUnsigned(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right) {
        int length = Math.Min(left.Length, right.Length);
        for(int i = 0; i < length; i++) {
            int comparison = left[i].CompareTo(right[i]);
            if(comparison != 0)
                return comparison;
        }

        return left.Length.CompareTo(right.Length);
    }
}