using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Serialization.Attributes {

    /// <summary>
    /// Attribute to Ignore Field in Serialization, some times need data in json but not need in parquet.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class ParquetIgnoreAttribute : Attribute {

    }
}
