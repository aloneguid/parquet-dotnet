using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Specifies that a property of type <see cref="DateTime"/> should be serialized as Parquet timestamp, which is internally
    /// an int64 number.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetTimestampAttribute : Attribute {
    }
}
