using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Specifies that <see cref="TimeSpan"/> field should be serialised with microseconds precision (not default milliseconds).
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetMicroSecondsTimeAttribute : Attribute {
    }
}
