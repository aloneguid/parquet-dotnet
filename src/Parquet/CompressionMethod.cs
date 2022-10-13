namespace Parquet {
    /// <summary>
    /// Parquet compression method
    /// </summary>
    public enum CompressionMethod {
        /// <summary>
        /// No compression
        /// </summary>
        None = 0,

        /// <summary>
        /// Snappy compression 
        /// </summary>
        Snappy = 1,

        /// <summary>
        /// Gzip compression
        /// </summary>
        Gzip = 2,

        /// <summary>
        /// LZO
        /// </summary>
        Lzo = 3,

        /// <summary>
        /// Brotli
        /// </summary>
        Brotli = 4,

        /// <summary>
        /// LZ4
        /// </summary>
        LZ4 = 5,

        /// <summary>
        /// ZSTD
        /// </summary>
        Zstd = 6,

        /// <summary>
        /// LZ4 raw
        /// </summary>
        Lz4Raw = 7,
    }
}
