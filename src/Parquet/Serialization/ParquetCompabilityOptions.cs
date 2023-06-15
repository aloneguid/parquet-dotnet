using System;

namespace Parquet.Serialization {
    /// <summary>
    /// Specifies flag controlling behavior of reader/writer to ensure schema compability
    /// </summary>
    [Flags]
    public enum ParquetCompabilityOptions {
        /// <summary>
        /// Use arrays to encode collections of primitive types
        /// </summary>
        MakeArrays = 0b0000_0001,

        /// <summary>
        /// Uses latest behaviour without ensuring that read/write is backward compatible
        /// </summary>
        Latest = 0b0000_0000
    }

    /// <summary>
    /// Defines extension method for <see cref="ParquetCompabilityOptions"/> for simplifying usage
    /// </summary>
    public static class ParquetCompabilityOptionsExtensions {
        /// <summary>
        /// Checks if flag is set on <see cref="ParquetCompabilityOptions"/>
        /// same way as <see cref="System.Enum.HasFlag"/> but without the overhead of boxing the parameter.
        /// </summary>
        /// <param name="options">Options to check against.</param>
        /// <param name="flag">Searched flags pattern.</param>
        public static bool IsSet(this ParquetCompabilityOptions options, ParquetCompabilityOptions flag)
            => (options & flag) == flag;

        /// <summary>
        /// Shorthand for <see cref="IsSet"/> checking for <see cref="ParquetCompabilityOptions.MakeArrays"/> flag.
        /// </summary>
        /// <param name="options">Options to check against.</param>
        public static bool IsMakeArraysSet(this ParquetCompabilityOptions options)
            => options.IsSet(ParquetCompabilityOptions.MakeArrays);
    }
}