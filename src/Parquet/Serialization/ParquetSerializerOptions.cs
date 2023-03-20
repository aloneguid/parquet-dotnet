using System.IO.Compression;

namespace Parquet.Serialization {
    /// <summary>
    /// Parquet serializer options
    /// </summary>
    public class ParquetSerializerOptions {
        /// <summary>
        /// Page compression method
        /// </summary>
        public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;


        /// <summary>
        /// Page compression level
        /// </summary>

        public CompressionLevel CompressionLevel = CompressionLevel.Optimal;
    }
}
