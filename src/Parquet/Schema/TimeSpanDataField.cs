using System;

namespace Parquet.Schema
{
   /// <summary>
   /// Schema element for <see cref="TimeSpan"/> which allows to specify precision
   /// </summary>
   public class TimeSpanDataField : DataField
   {
      /// <summary>
      /// Desired data format, Parquet specific
      /// </summary>
      public TimeSpanFormat TimeSpanFormat { get; }

      /// <summary>
      /// Initializes a new instance of the <see cref="TimeSpanDataField"/> class.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <param name="format">The format.</param>
      /// <param name="hasNulls">Is 'TimeSpan?'</param>
      /// <param name="isArray"></param>
      public TimeSpanDataField(string name, TimeSpanFormat format, bool hasNulls = true, bool isArray = false)
         : base(name, DataType.TimeSpan, hasNulls, isArray)
      {
         TimeSpanFormat = format;
      }
   }
}