using System;

namespace Parquet.Data
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
      /// <param name="isArray"></param>
      public DateTimeDataField(string name, DateTimeFormat format, bool hasNulls = true, bool isArray = false)
         : base(name, DataType.DateTimeOffset, hasNulls, isArray)
      {
         DateTimeFormat = format;
      }
   }
}