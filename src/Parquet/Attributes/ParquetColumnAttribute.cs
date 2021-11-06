using System;
using Parquet.Data;

namespace Parquet.Attributes
{
   /// <summary>
   /// Annotates a class property to provide some extra metadata for it.
   /// </summary>
   [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
   public class ParquetColumnAttribute : Attribute
   {
      /// <summary>
      /// Creates a new instance of the attribute class
      /// </summary>
      public ParquetColumnAttribute()
      {
         //Make the Defaults Explicit (vs implicit by simply being the first Enum); this helps make the code easier to reason about decrease risk from future changes.
         TimeSpanFormat = TimeSpanFormat.MilliSeconds;
         DateTimeFormat = DateTimeFormat.Impala;

         //NOTE: We implement the original defaults from parquet-dotnet before release v3.9 to achieve proper backwards compatibility!
         DecimalPrecision = DecimalFormatDefaults.DefaultPrecision;
         DecimalScale = DecimalFormatDefaults.DefaultScale;
      }

      /// <summary>
      /// Creates a new instance of the attribute class specifying column name
      /// </summary>
      /// <param name="name">Column name</param>
      public ParquetColumnAttribute(string name)
         : this()
      {
         Name = name;
      }

      /// <summary>
      /// Column name. When undefined a default property name is used which is simply the declared property name on the class.
      /// </summary>
      public string Name { get; set; }

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
   }
}