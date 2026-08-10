using System;
using Parquet.Schema;

namespace Parquet.Serialization.Attributes;

/// <summary>
/// Customize the time format of a time property. Applicable to <see cref="TimeSpan"/>, <see cref="TimeOnly"/>, <see cref="int"/> and <see cref="long"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ParquetTimeAttribute : Attribute {
    /// <summary>
    /// Indicates if the time is adjusted to UTC.
    /// </summary>
    public bool IsAdjustedToUtc { get; set; } = true;
    
    /// <summary>
    /// Time precision.
    /// </summary>
    public TimeUnitPrecision Precision { get; set; } = TimeUnitPrecision.Millis;

    internal static void Discover(Type elementType, ParquetTimeAttribute? attr, out TimeUnitPrecision precision, out bool isAdjustedToUtc) {
        if(elementType == typeof(TimeOnly)) {
            // TimeOnly can only store millis and micros
            if(attr == null) {
                precision = TimeUnitPrecision.Micros;   // default to highest precision possible for TimeOnly
                isAdjustedToUtc = true;
                return;
            }

            if(attr.Precision is TimeUnitPrecision.Millis or TimeUnitPrecision.Micros) {
                precision = attr.Precision;
                isAdjustedToUtc = attr.IsAdjustedToUtc;
                return;
            }

            throw new TypeAccessException($"{nameof(TimeOnly)} only supports {nameof(TimeUnitPrecision.Millis)} and {nameof(TimeUnitPrecision.Micros)} and not {attr.Precision}.");
        }

        if(elementType == typeof(int)) {
            if(attr == null) {
                precision = TimeUnitPrecision.Millis;
                isAdjustedToUtc = true;
                return;
            }

            if(attr.Precision == TimeUnitPrecision.Millis) {
                precision = TimeUnitPrecision.Millis;
                isAdjustedToUtc = attr.IsAdjustedToUtc;
                return;
            }

            throw new TypeAccessException($"{typeof(int)} only supports {nameof(TimeUnitPrecision.Millis)} precision and not {attr.Precision}");
        }

        if(elementType == typeof(long)) {
            if(attr == null) {
                precision = TimeUnitPrecision.Nanos;
                isAdjustedToUtc = true;
                return;
            }

            precision = attr.Precision;
            isAdjustedToUtc = attr.IsAdjustedToUtc;
            return;
        }

        throw new TypeAccessException($"Time is not compatible with {elementType} type.");
    }
}