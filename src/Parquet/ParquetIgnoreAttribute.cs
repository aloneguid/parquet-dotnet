using System;

namespace Parquet {
    /// <summary>
    /// Annotates a class property as a marker to ignore while serializing to parquet file
    /// </summary>
    [Obsolete("Prefer using JsonIgnore attribute, see https://github.com/aloneguid/parquet-dotnet/blob/master/docs/serialisation.md#customising-serialisation")]
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetIgnoreAttribute : Attribute {
    }
}