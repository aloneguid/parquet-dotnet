using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Changes column optionality to "required" for a list element.
    /// Only applicable to list definition, and applies to the list elements themselves, not the list class properties.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetListElementRequiredAttribute : Attribute {
    }
}
