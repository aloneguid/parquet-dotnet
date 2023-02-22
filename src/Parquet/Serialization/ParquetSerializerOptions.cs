using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Text;

namespace Parquet.Serialization {
    internal class ParquetSerializerOptions {
        public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

        public CompressionLevel CompressionLevel = CompressionLevel.Optimal;
    }
}
