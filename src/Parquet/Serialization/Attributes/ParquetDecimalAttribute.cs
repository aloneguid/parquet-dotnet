using System;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Annotates a <see cref="decimal"/> property to allow customizing precision and scale
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetDecimalAttribute : Attribute {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="precision"></param>
        /// <param name="scale"></param>
        public ParquetDecimalAttribute(int precision, int scale) {
            Precision = precision;
            Scale = scale;
        }

        /// <summary>
        /// Precision
        /// </summary>
        public int Precision { get; }

        /// <summary>
        /// Scale
        /// </summary>
        public int Scale { get; }
    }
}
