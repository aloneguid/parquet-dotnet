namespace Parquet.Data {
    /// <summary>
    /// Pre-defined decimal handling default values; providing backwards compatibility prior to v3.9 where these were made configurable.
    /// </summary>
    public static class DecimalFormatDefaults {

        //todo : move to ParquetOptions

        /// <summary>
        /// The Default Precision value used when not explicitly defined; this is the value used prior to parquet-dotnet v3.9.
        /// </summary>
        public const int DefaultPrecision = 38;

        /// <summary>
        /// The Default Scale value used when not explicitly defined; this is the value used prior to parquet-dotnet v3.9.
        /// </summary>
        public const int DefaultScale = 18;
    }
}
