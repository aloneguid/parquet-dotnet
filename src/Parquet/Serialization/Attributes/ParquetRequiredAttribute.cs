using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Changes column optionality to "required".
    /// This is used to make string properties non-nullable, as in .NET strings are nullable by default.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetRequiredAttribute : Attribute {
    }
}
