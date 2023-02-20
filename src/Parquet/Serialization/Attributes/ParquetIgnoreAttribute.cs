using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Prevents a property from being serialized or deserialized.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetIgnoreAttribute : Attribute {
    }
}
