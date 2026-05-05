using System;
using System.Collections.Generic;
using System.IO.Compression;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet;

/// <summary>
/// Parquet options
/// </summary>
public class ParquetOptions {

    /// <summary>
    /// Compression method to use when writing, defaults to <see cref="CompressionMethod.Snappy"/>
    /// </summary>
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

    /// <summary>
    /// Compression level to use when writing, defaults to <see cref="CompressionLevel.SmallestSize"/>. The actual
    /// physical compression level varies based on compression algorithm.
    /// </summary>
    public CompressionLevel CompressionLevel = CompressionLevel.SmallestSize;

    /// <summary>
    /// When true byte arrays will be treated as UTF-8 strings when reading files.
    /// </summary>
    public bool TreatByteArrayAsString { get; set; } = false;

    /// <summary>
    /// When true, big integers are always treated as dates when reading files.
    /// </summary>
    public bool TreatBigIntegersAsDates { get; set; } = true;

    /// <summary>
    /// When set to true, parquet dates will be deserialized as <see cref="DateOnly"/>, otherwise as
    /// <see cref="DateTime"/> with missing time part.
    /// </summary>
    public bool UseDateOnlyTypeForDates { get; set; } = false;

    /// <summary>
    /// When set to true, parquet times with millisecond precision will be deserialized as <see cref="TimeOnly"/>,
    /// otherwise as <see cref="TimeSpan"/> with missing time part.
    /// </summary>
    public bool UseTimeOnlyTypeForTimeMillis { get; set; } = false;

    /// <summary>
    /// When set to true, parquet times with microsecond precision will be deserialized as <see cref="TimeOnly"/>,
    /// otherwise as <see cref="TimeSpan"/> with missing time part.
    /// </summary>
    public bool UseTimeOnlyTypeForTimeMicros { get; set; } = false;

    /// <summary>
    /// Specifies hints to the writers about which encoding to use for specific columns. To get column path use
    /// <see cref="Field.Path"/> on <see cref="DataField"/>.
    /// </summary>
    public readonly IDictionary<string, EncodingHint> ColumnEncodingHints = new Dictionary<string, EncodingHint>();

    internal EncodingHint GetEncodingHint(DataField df) {
        return ColumnEncodingHints.TryGetValue(df.Path.ToString(), out EncodingHint hint) ? hint : EncodingHint.Default;
    }

    /// <summary>
    /// Dictionary uniqueness threshold, which is a value from 0 (no unique values) to 1 (all values are unique)
    /// indicating when dictionary encoding is applied. Uniqueness factor needs to be less or equal than this threshold.
    /// </summary>
    public double DictionaryEncodingThreshold { get; set; } = 0.8;

    /// <summary>
    /// Number of values to sample before attempting full dictionary encoding.
    /// When the column has more values than this limit, a quick uniqueness check is performed
    /// on the first <c>DictionaryEncodingSampleSize</c> values. If the sample exceeds
    /// <see cref="DictionaryEncodingThreshold"/>, dictionary encoding is skipped entirely,
    /// avoiding the expensive full-data scan.
    /// Set to 0 to disable sampling and always scan the full column.
    /// Default is 0 (disabled - full column scan, preserving pre-existing behavior).
    /// </summary>
    public int DictionaryEncodingSampleSize { get; set; } = 0;

    /// <summary>
    /// This option is passed to the <see cref="Microsoft.IO.RecyclableMemoryStreamManager"/>, which keeps a pool of
    /// streams in memory for reuse. By default when this option is unset, the RecyclableStreamManager will keep an
    /// unbounded amount of memory, which is "indistinguishable from a memory leak" per their documentation. This does
    /// not restrict the size of the pool, but just allows the garbage collector to free unused memory over this limit.
    /// You may want to adjust this smaller to reduce max memory usage, or larger to reduce garbage collection
    /// frequency. Defaults to 16MB.
    /// </summary>
    public int MaximumSmallPoolFreeBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// This option is passed to the <see cref="Microsoft.IO.RecyclableMemoryStreamManager"/>, which keeps a pool of
    /// streams in memory for reuse. By default when this option is unset, the RecyclableStreamManager will keep an
    /// unbounded amount of memory, which is "indistinguishable from a memory leak" per their documentation. This does
    /// not restrict the size of the pool, but just allows the garbage collector to free unused memory over this limit.
    /// You may want to adjust this smaller to reduce max memory usage, or larger to reduce garbage collection
    /// frequency. Defaults to 64MB.
    /// </summary>
    public int MaximumLargePoolFreeBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    /// When true, decimals will be read and written as <see cref="BigDecimal"/> instead of <see cref="decimal"/>. This
    /// is required if you are working with truly large decimals.
    /// </summary>
    public bool UseBigDecimal { get; set; } = false;

    internal Type DecimalType => UseBigDecimal ? typeof(BigDecimal) : typeof(decimal);

    /// <summary>
    /// The Default Precision value used when not explicitly defined; this is the value used prior to parquet-dotnet
    /// v3.9.
    /// </summary>
    public const int DefaultPrecision = 38;

    /// <summary>
    /// The Default Scale value used when not explicitly defined; this is the value used prior to parquet-dotnet v3.9.
    /// </summary>
    public const int DefaultScale = 18;

    /// <summary>
    /// If true, will attemp to use hardware accelerated math.
    /// </summary>
    public static bool UseHardwareAcceleration { get; set; } = true;

    #region [ Serializer specific]

    /// <summary>
    /// When set to true, appends to file by creating a new row group. Only applicable when using
    /// <see cref="ParquetSerializer"/>. Be careul not to create a lot of tiny row groups as this affects reading
    /// performance in all Parquet readers, not just this library.
    /// </summary>
    public bool Append { get; set; } = false;

    /// <summary>
    /// Default size of row groups if not specified
    /// </summary>
    public const int DefaultRowGroupSize = 1_000_000;

    /// <summary>
    /// Custom row group size, if different from <see cref="DefaultRowGroupSize"/> used by
    /// <see cref="ParquetSerializer"/>.
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

    #endregion
}
