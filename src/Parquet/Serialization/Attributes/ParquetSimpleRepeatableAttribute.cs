using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Specifies that a property is a simple repeatable field (as opposed to a list).
    /// Used only for legacy compatibility.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetSimpleRepeatableAttribute : Attribute {
    }
}
