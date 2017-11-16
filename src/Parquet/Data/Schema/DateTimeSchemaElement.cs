using System;
using Parquet.DataTypes;

namespace Parquet.Data
{
   /// <summary>
   /// Schema element for <see cref="DateTimeOffset"/> which allows to specify precision
   /// </summary>
   public class DateTimeSchemaElement : SchemaElement
   {
      public DateTimeFormat DateTimeFormat { get; }

      /// <summary>
      /// Initializes a new instance of the <see cref="DateTimeSchemaElement"/> class.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <param name="format">The format.</param>
      /// <param name="nullable">Is 'DateTime?'</param>
      /// <exception cref="ArgumentException">format</exception>
      public DateTimeSchemaElement(string name, DateTimeFormat format, bool hasNulls = true, bool isArray = false)
         : base(name, DataType.DateTimeOffset, hasNulls, isArray)
      {
         DateTimeFormat = format;
      }
   }
}