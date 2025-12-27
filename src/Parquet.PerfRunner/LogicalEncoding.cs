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
    public static ParquetOptions CreateOptions(this LogicalEncoding encoding) =>
      encoding switch {
          LogicalEncoding.RleDictionary => new ParquetOptions {
              UseDictionaryEncoding = true,
              // Force dictionary extraction for every supported column to match the ParquetSharp benchmark setup.
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
}
