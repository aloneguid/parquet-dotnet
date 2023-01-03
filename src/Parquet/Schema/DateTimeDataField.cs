using System;

namespace Parquet.Schema
{
   /// <summary>
   /// Schema element for <see cref="DateTimeOffset"/> which allows to specify precision
   /// </summary>
   public class DateTimeDataField : DataField
   {
      /// <summary>
      /// Desired data format, Parquet specific
      /// </summary>
      public DateTimeFormat DateTimeFormat { get; }

      /// <summary>
      /// Initializes a new instance of the <see cref="DateTimeDataField"/> class.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <param name="format">The format.</param>
      /// <param name="hasNulls">Is 'DateTime?'</param>
      /// <param name="isArray">When true, each value of this field can have multiple values, similar to array in C#.</param>
      /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
      public DateTimeDataField(string name, DateTimeFormat format, bool hasNulls = true, bool isArray = false, string propertyName = null)
         : base(name, DataType.DateTimeOffset, hasNulls, isArray, propertyName)
      {
         DateTimeFormat = format;
      }
   }
}