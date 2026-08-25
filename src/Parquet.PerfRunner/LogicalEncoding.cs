using Parquet.Schema;

namespace Parquet.PerfRunner;

/// <summary>
/// Allow to easily control the logical encoding used in benchmarks.
/// </summary>
public enum LogicalEncoding {
    Plain,
    RleDictionary,
    DeltaBinaryPacked
}

public static class LogicalEncodingExtensions {
#if PARQUET_PACKAGE
    public static ParquetOptions CreateOptions(this LogicalEncoding encoding, ParquetSchema? schema = null) =>
      encoding switch {
          LogicalEncoding.RleDictionary => new ParquetOptions {
              UseDictionaryEncoding = true,
              // Force dictionary extraction for every supported column to match the local benchmark setup.
              DictionaryEncodingThreshold = double.MaxValue,
              UseDeltaBinaryPackedEncoding = false
          },
          LogicalEncoding.DeltaBinaryPacked => new ParquetOptions {
              UseDictionaryEncoding = false,
              UseDeltaBinaryPackedEncoding = true
          },
          LogicalEncoding.Plain => new ParquetOptions {
              UseDictionaryEncoding = false,
              UseDeltaBinaryPackedEncoding = false
          },
          _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unknown logical encoding")
      };
#else
    public static ParquetOptions CreateOptions(this LogicalEncoding encoding, ParquetSchema? schema = null) {
        ParquetOptions options = encoding switch {
            LogicalEncoding.RleDictionary => new ParquetOptions {
                CompressionMethod = CompressionMethod.None,
                // Force dictionary extraction for every supported column to match the ParquetSharp benchmark setup.
                DictionaryEncodingThreshold = double.MaxValue
            },
            LogicalEncoding.DeltaBinaryPacked => new ParquetOptions {
                CompressionMethod = CompressionMethod.None
            },
            LogicalEncoding.Plain => new ParquetOptions {
                CompressionMethod = CompressionMethod.None,
                DictionaryEncodingThreshold = -1
            },
            _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unknown logical encoding")
        };

        if(schema != null) {
            EncodingHint hint = encoding switch {
                LogicalEncoding.RleDictionary => EncodingHint.Dictionary,
                LogicalEncoding.DeltaBinaryPacked => EncodingHint.DeltaBinaryPacked,
                LogicalEncoding.Plain => EncodingHint.Default,
                _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unknown logical encoding")
            };

            foreach(DataField field in schema.GetDataFields()) {
                options.ColumnEncodingHints[field.Path.ToString()] = hint;
            }
        }

        return options;
    }
#endif
}
