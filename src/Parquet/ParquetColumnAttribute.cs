using System;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Annotates a class property to provide some extra metadata for it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ParquetColumnAttribute : Attribute {
        /// <summary>
        /// Creates a new instance of the attribute class
        /// </summary>
        public ParquetColumnAttribute() {
            //Make the Defaults Explicit (vs implicit by simply being the first Enum); this helps make the code easier
            // to reason about decrease risk from future changes.
            TimeSpanFormat = TimeSpanFormat.MilliSeconds;
            DateTimeFormat = DateTimeFormat.Impala;

            //NOTE: We implement the original defaults from parquet-dotnet before release v3.9 to achieve proper backwards compatibility!
            DecimalPrecision = DecimalFormatDefaults.DefaultPrecision;
            DecimalScale = DecimalFormatDefaults.DefaultScale;

            ListContainerName = ListField.DefaultContainerName;
        }

        /// <summary>
        /// Creates a new instance of the attribute class specifying column name
        /// </summary>
        /// <param name="name">Column name</param>
        public ParquetColumnAttribute(string name)
           : this() {
            Name = name;
        }

        /// <summary>
        /// Column name. When undefined a default property name is used which is simply the declared property name on the class.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// TmeSpanFormat. MilliSeconds or MicroSeconds
        /// </summary>
        public TimeSpanFormat TimeSpanFormat { get; set; }

        /// <summary>
        /// DateTimeFormat. Impala or DateAndTime or Date
        /// </summary>
        public DateTimeFormat DateTimeFormat { get; set; }

        /// <summary>
        /// Precision for decimal fields
        /// </summary>
        public int DecimalPrecision { get; set; }

        /// <summary>
        /// Scale for decimal fields
        /// </summary>
        public int DecimalScale { get; set; }

        /// <summary>
        /// Should this decimal field force byte array encoding?
        /// </summary>
        public bool DecimalForceByteArrayEncoding { get; set; }

        /// <summary>
        /// Should this field be generated as a <see cref="ListField"/>?
        /// </summary>
        public bool UseListField { get; set; }

        /// <summary>
        /// Name of the conatiner for the list. Path will be Name.ListContainerName.ListElementName
        /// </summary>
        public string ListContainerName { get; set; }

        /// <summary>
        /// Name of the element for the list. Path will be Name.ListContainerName.ListElementName
        /// </summary>
        public string? ListElementName { get; set; }
    }
}