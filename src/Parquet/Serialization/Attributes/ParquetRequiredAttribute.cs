using System;

namespace Parquet.Serialization.Attributes;

/// <summary>
/// Changes column optionality to "required".
/// This is used to make string properties non-nullable, as in .NET strings are nullable by default.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ParquetRequiredAttribute : Attribute {
    /// <inheritdoc />
    public ParquetRequiredAttribute(bool isRequired = true) => IsRequired = isRequired;

    /// <summary>
    /// Whether the column is required.
    /// </summary>
    public bool IsRequired { get; }
}
