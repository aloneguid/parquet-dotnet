using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Marks property as Parquet Map type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetMapAttribute : Attribute {
    }
}
