using Parquet.File;
using System;

namespace Parquet.Data
{
   /// <summary>
   /// Element of dataset's schema
   /// <typeparamref name="T">Type of element in the column</typeparamref>
   /// </summary>
   public class SchemaElement<T> : SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      public SchemaElement(string name) : base(name, typeof(T))
      {
         
      }
   }

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
      /// <exception cref="ArgumentException">format</exception>
      public DateTimeSchemaElement(string name, DateTimeFormat format) : base(name, typeof(DateTimeOffset))
      {
         switch(format)
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


   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public class SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="elementType">Type of the element in this column</param>
      public SchemaElement(string name, Type elementType)
      {
         if (string.IsNullOrEmpty(name))
            throw new ArgumentException("cannot be null or empty", nameof(name));

         Name = name;
         ElementType = elementType;
         Thrift = new Thrift.SchemaElement(name)
         {
            //this must be changed later or if column has nulls (on write)
            Repetition_type = Parquet.Thrift.FieldRepetitionType.REQUIRED
         };
         TypeFactory.AdjustSchema(Thrift, elementType);
      }

      internal SchemaElement(Thrift.SchemaElement thriftSchema)
      {
         Name = thriftSchema.Name;
         Thrift = thriftSchema;
         ElementType = TypeFactory.ToSystemType(thriftSchema);
      }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; }

      /// <summary>
      /// Element type
      /// </summary>
      public Type ElementType { get; }

      /// <summary>
      /// Returns true if element can have null values
      /// </summary>
      public bool IsNullable
      {
         get { return Thrift.Repetition_type != Parquet.Thrift.FieldRepetitionType.REQUIRED; }
         set { Thrift.Repetition_type = value ? Parquet.Thrift.FieldRepetitionType.OPTIONAL : Parquet.Thrift.FieldRepetitionType.REQUIRED; }
      }

      internal Thrift.SchemaElement Thrift { get; set; }

      internal bool IsAnnotatedWith(Thrift.ConvertedType ct)
      {
         //checking __isset is important, this is a way of Thrift to tell whether the variable is set at all
         return Thrift.__isset.converted_type && Thrift.Converted_type == ct;
      }

      /// <summary>
      /// Detect if data page has definition levels written.
      /// </summary>
      internal bool HasDefinitionLevelsPage(Thrift.PageHeader ph)
      {
         if (!Thrift.__isset.repetition_type)
            throw new ParquetException("repetiton type is missing");

         return Thrift.Repetition_type != Parquet.Thrift.FieldRepetitionType.REQUIRED;
      }

      /// <summary>
      /// Pretty prints
      /// </summary>
      public override string ToString()
      {
         return $"{Name} ({ElementType}), nullable: {IsNullable}";
      }
   }
}