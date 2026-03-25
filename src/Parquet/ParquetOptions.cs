using System;
using System.Collections.Generic;
using System.Data;
using Parquet.Data;

namespace Parquet {
    /// <summary>
    /// Parquet options
    /// </summary>
    public class ParquetOptions {
        
        /// <summary>
        /// When true byte arrays will be treated as UTF-8 strings on read
        /// </summary>
        public bool TreatByteArrayAsString { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether big integers are always treated as dates on read
        /// </summary>
        public bool TreatBigIntegersAsDates { get; set; } = true;

#if NET6_0_OR_GREATER
        /// <summary>
        /// When set to true, parquet dates will be deserialized as <see cref="DateOnly"/>, otherwise
        /// as <see cref="DateTime"/> with missing time part.
        /// </summary>
        public bool UseDateOnlyTypeForDates { get; set; } = false;

        /// <summary>
        /// When set to true, parquet times with millisecond precision will be deserialized as <see cref="TimeOnly"/>, otherwise
        /// as <see cref="TimeSpan"/> with missing time part.
        /// </summary>
        public bool UseTimeOnlyTypeForTimeMillis { get; set; } = false;

        /// <summary>
        /// When set to true, parquet times with microsecond precision will be deserialized as <see cref="TimeOnly"/>, otherwise
        /// as <see cref="TimeSpan"/> with missing time part.
        /// </summary>
        public bool UseTimeOnlyTypeForTimeMicros { get; set; } = false;
#endif

        /// <summary>
        /// Whether to use dictionary encoding for columns if data meets <seealso cref="DictionaryEncodingThreshold"/>
        /// The following CLR types are currently supported:
        /// <see cref="string"/>, <see cref="DateTime"/>, <see cref="decimal"/>, <see cref="byte"/>, <see cref="short"/>, <see cref="ushort"/>, <see cref="int"/>, <see cref="uint"/>, <see cref="long"/>, <see cref="ulong"/>, <see cref="float"/>, <see cref="double"/>"/>
        /// </summary>
        public bool UseDictionaryEncoding { get; set; } = true;

        /// <summary>
        /// Dictionary uniqueness threshold, which is a value from 0 (no unique values) 
        /// to 1 (all values are unique) indicating when dictionary encoding is applied.
        /// Uniqueness factor needs to be less or equal than this threshold.
        /// </summary>
        public double DictionaryEncodingThreshold { get; set; } = 0.8;

        /// <summary>
        /// Per-column dictionary encoding overrides. When set, columns whose path (using "/" separator)
        /// matches a key in this dictionary will use the specified value instead of the global
        /// <see cref="UseDictionaryEncoding"/> setting. For example, <c>{ "Name" : true, "Address/City" : false }</c>
        /// forces dictionary encoding on for the "Name" column and off for the nested "City" column,
        /// regardless of the global setting.
        /// Similar to PyArrow's <c>column_encoding</c> parameter.
        /// This dictionary must not be modified after being assigned to a <see cref="ParquetOptions"/> instance
        /// that is in use by a writer.
        /// </summary>
        public IReadOnlyDictionary<string, bool>? ColumnDictionaryEncodings { get; set; }

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
        /// When set, the default encoding for INT32 and INT64 is <see cref="Parquet.Meta.Encoding.DELTA_BINARY_PACKED"/>, otherwise
        /// it's reverted to <see cref="Parquet.Meta.Encoding.PLAIN"/>. You should only set this to <see langword="false"/> if
        /// your readers do not understand it.
        /// </summary>
        public bool UseDeltaBinaryPackedEncoding { get; set; } = true;

        /// <summary>
        /// This option is passed to the <see cref="Microsoft.IO.RecyclableMemoryStreamManager"/> , 
        /// which keeps a pool of streams in memory for reuse. 
        /// By default when this option is unset, the RecyclableStreamManager 
        /// will keep an unbounded amount of memory, which is 
        /// "indistinguishable from a memory leak" per their documentation.
        /// 
        /// This does not restrict the size of the pool, but just allows 
        /// the garbage collector to free unused memory over this limit.
        /// 
        /// You may want to adjust this smaller to reduce max memory usage, 
        /// or larger to reduce garbage collection frequency.
        /// 
        /// Defaults to 16MB.  
        /// </summary>
        public int MaximumSmallPoolFreeBytes { get; set; } = 16 * 1024 * 1024;

        /// <summary>
        /// This option is passed to the <see cref="Microsoft.IO.RecyclableMemoryStreamManager"/> , 
        /// which keeps a pool of streams in memory for reuse. 
        /// By default when this option is unset, the RecyclableStreamManager 
        /// will keep an unbounded amount of memory, which is 
        /// "indistinguishable from a memory leak" per their documentation.
        /// 
        /// This does not restrict the size of the pool, but just allows 
        /// the garbage collector to free unused memory over this limit.
        /// 
        /// You may want to adjust this smaller to reduce max memory usage, 
        /// or larger to reduce garbage collection frequency.
        /// 
        /// Defaults to 64MB.
        /// </summary>
        public int MaximumLargePoolFreeBytes { get; set; } = 64 * 1024 * 1024;

        /// <summary>
        /// When true, decimals will be read and written as <see cref="BigDecimal"/> instead of <see cref="decimal"/>. This is required if you are working with truly large decimals.
        /// </summary>
        public bool UseBigDecimal { get; set; } = false;

        internal Type DecimalType => UseBigDecimal ? typeof(BigDecimal) : typeof(decimal);
    }
}
