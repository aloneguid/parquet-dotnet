using System;

namespace Parquet.Data
{
   /// <summary>
   /// Schema element for <see cref="DateTimeOffset"/> which allows to specify precision
   /// </summary>
   public class DateTimeSchemaElement : SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="DateTimeSchemaElement"/> class.
      /// </summary>
      /// <param name="name">The name.</param>
      /// <param name="format">The format.</param>
      /// <param name="nullable">Is 'DateTime?'</param>
      /// <exception cref="ArgumentException">format</exception>
      public DateTimeSchemaElement(string name, DateTimeFormat format, bool nullable = false) : base(name, nullable)
      {
         ElementType = ColumnType = typeof(DateTimeOffset);
         switch (format)
         {
            case DateTimeFormat.Impala:
               Thrift.Type = Parquet.Thrift.Type.INT96;
               Thrift.Converted_type = Parquet.Thrift.ConvertedType.TIMESTAMP_MILLIS;
               break;
            case DateTimeFormat.DateAndTime:
               Thrift.Type = Parquet.Thrift.Type.INT64;
               Thrift.Converted_type = Parquet.Thrift.ConvertedType.TIMESTAMP_MILLIS;
               break;
            case DateTimeFormat.Date:
               Thrift.Type = Parquet.Thrift.Type.INT32;
               Thrift.Converted_type = Parquet.Thrift.ConvertedType.DATE;
               break;
            default:
               throw new ArgumentException($"unknown date format '{format}'", nameof(format));
         }
      }
   }
}