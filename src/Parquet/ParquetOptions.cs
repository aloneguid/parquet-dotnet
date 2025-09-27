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
        /// Key used to decrypt encrypted parquet files created in encrypted footer mode.
        /// </summary>
        /// <remarks>Currently only used by <see cref="ParquetReader"/></remarks>
        public string? SecretKey { get; set; } = null;

        /// <summary>
        /// Optional Additional Authentication Data Prefix used to verify the integrity of the encrypted file. Only required
        /// if the file was encrypted with an AAD Prefix *and* the prefix wasn't embedded into the 
        /// file by the author.
        /// </summary>
        /// <remarks>Currently only used by <see cref="ParquetReader"/></remarks>
        public string? AADPrefix { get; set; } = null;

        /// <summary>
        /// Controls whether the writer embeds the AAD prefix in the file metadata
        /// or requires readers to supply it out-of-band.
        /// </summary>
        /// <value>
        /// <c>false</c> (default): store the AAD prefix in the file (if provided in <see cref="AADPrefix"/>).<br/>
        /// <c>true</c>: do not store the prefix; readers must provide the same prefix to decrypt.
        /// </value>
        /// <remarks>
        /// <para>
        /// When <c>true</c>, <see cref="AADPrefix"/> must be set at write time. During read, the same prefix
        /// must be provided in <see cref="AADPrefix"/>; otherwise decryption fails with an explicit error.
        /// </para>
        /// <para>
        /// This maps to the Parquet encryption algorithm field <c>supply_aad_prefix</c>.
        /// </para>
        /// </remarks>
        public bool SupplyAadPrefix { get; set; } = false;

        /// <summary>
        /// Use the AES-GCM-CTR variant for page bodies (per Parquet modular encryption spec).
        /// </summary>
        /// <value>
        /// <c>false</c> (default): all modules use AES-GCM (V1).<br/>
        /// <c>true</c>: page <b>bodies</b> use AES-CTR framing; page headers and all other modules remain AES-GCM.
        /// </value>
        /// <remarks>
        /// <para>
        /// Regardless of this setting, the file <b>footer</b> is always encrypted with AES-GCM.
        /// </para>
        /// <para>
        /// Set this only if you need interoperability with writers/readers expecting the AES_GCM_CTR_V1 profile.
        /// </para>
        /// </remarks>
        public bool UseCtrVariant { get; set; } = false;

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
