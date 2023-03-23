using System.IO.Compression;

namespace Parquet.Serialization {
    /// <summary>
    /// Parquet serializer options
    /// </summary>
    public class ParquetSerializerOptions {

        /// <summary>
        /// When set to true, appends to file by creating a new row group.
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
    }
}
