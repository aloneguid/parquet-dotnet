using System.IO.Compression;

namespace Parquet.Serialization;

/// <summary>
/// Parquet serializer options
/// </summary>
public class ParquetSerializerOptions {

    /// <summary>
    /// When set to true, appends to file by creating a new row group. Be careful not to create a lot of tiny row groups
    /// as this affects reading performance in all Parquet readers, not just this library.
    /// </summary>
    public bool Append { get; set; } = false;

    /// <summary>
    /// Page compression method
    /// </summary>
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

    /// <summary>
    /// Page compression level
    /// </summary>

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;

    /// <summary>
    /// Default size of row groups if not specified
    /// </summary>
    public const int DefaultRowGroupSize = 1_000_000;

    /// <summary>
    /// Custom row group size, if different from <see cref="DefaultRowGroupSize"/>.
    /// </summary>
    public int? RowGroupSize { get; set; }

    /// <summary>
    /// Gets or sets a value that indicates whether a property's name uses a case-insensitive comparison during
    /// deserialization. The default value is false. Full credits to
    /// https://learn.microsoft.com/en-us/dotnet/api/system.text.json.jsonserializeroptions.propertynamecaseinsensitive?view=net-8.0#system-text-json-jsonserializeroptions-propertynamecaseinsensitive
    /// </summary>
    public bool PropertyNameCaseInsensitive { get; set; } = false;

    /// <summary>
    /// When using untyped serialisation, prefers using <see cref="System.String"/> when reading string values, instead
    /// of <see cref="System.ReadOnlyMemory{Char}"/>.
    /// </summary>
    public static bool PreferUntypedString { get; set; } = true;

    /// <summary>
    /// When using untyped serialisation, prefers using <see cref="byte"/> array when reading byte array values, instead
    /// of <see cref="System.ReadOnlyMemory{Byte}"/>.
    /// </summary>
    public static bool PreferUntypedByteArray { get; set; } = true;

    /// <summary>
    /// Further customisations for lower level Parquet options.
    /// </summary>
    public ParquetOptions? ParquetOptions { get; set; } = new ParquetOptions();
}
