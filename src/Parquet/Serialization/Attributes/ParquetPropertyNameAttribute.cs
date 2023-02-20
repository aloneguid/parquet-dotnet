using System;

namespace Parquet.Serialization.Attributes {
    /// <summary>
    /// Specifies the property name that is present in the shema when serializing and deserializing.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetPropertyNameAttribute : Attribute {

        /// <summary>
        /// Creates a new instance of the attribute class specifying column name
        /// </summary>
        /// <param name="name">Column name</param>
        public ParquetPropertyNameAttribute(string name) { 
            Name = name;
        }

        /// <summary>
        /// Column name. When undefined a default property name is used which is simply the declared property name on the class.
        /// </summary>
        public string Name { get; set; }
    }
}