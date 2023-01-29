using System;

namespace Parquet.Schema {
    /// <summary>
    /// List of supported data types
    /// </summary>
    [Obsolete("Please remove references to this enum and use System.Type where appropriate. WIll be removed in the next major release.")]
    public enum DataType {
        /// <summary>
        /// Type is not specified, shouldn't be used.
        /// </summary>
        Unspecified,

        /// <summary>
        /// Boolean
        /// </summary>
        Boolean,

        /// <summary>
        /// Byte (unsigned)
        /// </summary>
        Byte,

        /// <summary>
        /// Signed byte
        /// </summary>
        SignedByte,

        /// <summary>
        /// 16 bit integer
        /// </summary>
        Int16,

        /// <summary>
        /// 16 bit unsigned integer
        /// </summary>
        UnsignedInt16,

        /// <summary>
        /// 32 bit integer
        /// </summary>
        Int32,

        /// <summary>
        /// 32 bit unsigned integer
        /// </summary>
        UnsignedInt32,

        /// <summary>
        /// 64 bit integer
        /// </summary>
        Int64,

        /// <summary>
        /// 64 bit unsigned integer
        /// </summary>
        UnsignedInt64,

        /// <summary>
        /// 96 bit integer
        /// </summary>
        Int96,

        /// <summary>
        /// Array of bytes
        /// </summary>
        ByteArray,

        /// <summary>
        /// UTF-8 string
        /// </summary>
        String,

        /// <summary>
        /// Float
        /// </summary>
        Float,

        /// <summary>
        /// Double
        /// </summary>
        Double,

        /// <summary>
        /// Decimal
        /// </summary>
        Decimal,

        /// <summary>
        /// DateTimeOffset
        /// </summary>
        DateTimeOffset,

        /// <summary>
        /// Interval
        /// </summary>
        Interval,

        /// <summary>
        /// TimeSpan
        /// </summary>
        TimeSpan
    }
}
