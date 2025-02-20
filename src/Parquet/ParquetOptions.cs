using System;
using System.Data;

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
    }
}
