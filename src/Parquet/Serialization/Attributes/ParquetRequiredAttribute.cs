using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Changes column optionality to "required".
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetRequiredAttribute : Attribute {
    }
}
