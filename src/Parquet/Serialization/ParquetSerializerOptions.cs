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

        /// <summary>
        /// Default size of row groups if not specified
        /// </summary>
        public const int DefaultRowGroupSize = 1_000_000;

        /// <summary>
        /// Custom row group size, if different from 
        /// </summary>
        public int? RowGroupSize { get; set; }

        /// <summary>
        /// Further customisations
        /// </summary>
        public ParquetOptions? ParquetOptions { get; set; } = new ParquetOptions();
    }
}
