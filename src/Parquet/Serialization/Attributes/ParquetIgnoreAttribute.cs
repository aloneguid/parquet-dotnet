using System;
using System.Text.Json.Serialization;

namespace Parquet.Serialization.Attributes;

/// <summary>
/// Attribute to ignore a field in class serialization. Behaves identical to <see cref="JsonIgnoreAttribute"/> and can be used interchangeably.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ParquetIgnoreAttribute : Attribute;
