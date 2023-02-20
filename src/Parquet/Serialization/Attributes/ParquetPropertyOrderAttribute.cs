using System;

namespace Parquet.Serialization.Attributes {
    /// <summary>
    /// Specifies the property name that is present in the shema when serializing and deserializing.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetPropertyOrderAttribute : Attribute {

        /// <summary>
        /// Creates a new instance of the attribute class specifying column order
        /// </summary>
        /// <param name="order">Column order</param>
        public ParquetPropertyOrderAttribute(int order) {
            Order = order;
        }

        /// <summary>
        /// Column name. When undefined a default property name is used which is simply the declared property name on the class.
        /// </summary>
        public int Order { get; set; }
    }
}