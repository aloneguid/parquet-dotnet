using Parquet.File;
using System;
using Parquet.File.Values;
using System.Collections.Generic;

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
      public SchemaElement(string name, SchemaElement parent = null) : base(name, typeof(T), parent)
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
      public DateTimeSchemaElement(string name, DateTimeFormat format) : base(name)
      {
         ElementType = typeof(DateTimeOffset);
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

   /// <summary>
   /// Maps onto Parquet Interval type 
   /// </summary>
   public class IntervalSchemaElement : SchemaElement
   {
      /// <summary>
      /// Constructs a parquet interval type
      /// </summary>
      /// <param name="name">The name of the column</param>
      public IntervalSchemaElement(string name) : base(name)
      {
         Thrift.Type = Parquet.Thrift.Type.FIXED_LEN_BYTE_ARRAY;
         Thrift.Converted_type = Parquet.Thrift.ConvertedType.INTERVAL;
         Thrift.Type_length = 12;
         ElementType = typeof(Interval);
      }
   }


   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public class SchemaElement : IEquatable<SchemaElement>
   {
      private readonly List<SchemaElement> _children = new List<SchemaElement>();

      /// <summary>
      /// Gets the children schemas
      /// </summary>
      public IList<SchemaElement> Children => _children;

      public SchemaElement Parent { get; private set; }

      /// <summary>
      /// Used by derived classes to invoke 
      /// </summary>
      /// <param name="name"></param>
      protected SchemaElement(string name)
      {
         Thrift = new Thrift.SchemaElement(name)
         {
            //this must be changed later or if column has nulls (on write)
            Repetition_type = Parquet.Thrift.FieldRepetitionType.REQUIRED
         };
         Name = name;
      }
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="elementType">Type of the element in this column</param>
      public SchemaElement(string name, Type elementType, SchemaElement parent = null)
      {
         SetProperties(name, elementType);
         TypeFactory.AdjustSchema(Thrift, elementType);
         Parent = parent;
      }

      private void SetProperties(string name, Type elementType)
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
      }

      internal SchemaElement(Thrift.SchemaElement thriftSchema, SchemaElement parent, ParquetOptions formatOptions, Type elementType)
      {
         Name = thriftSchema.Name;
         Thrift = thriftSchema;
         Parent = parent;
         ElementType = elementType ?? TypeFactory.ToSystemType(this, formatOptions);
      }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; private set; }

      /// <summary>
      /// Element type
      /// </summary>
      public Type ElementType { get; internal set; }

      public string Path
      {
         get
         {
            var parts = new List<string>();
            SchemaElement current = this;

            while (current != null)
            {
               parts.Add(current.Name);
               current = current.Parent;
            }

            parts.Reverse();
            return string.Join(".", parts);
         }
      }

      /// <summary>
      /// Returns true if element can have null values
      /// </summary>
      public bool IsNullable
      {
         get => Thrift.Repetition_type != Parquet.Thrift.FieldRepetitionType.REQUIRED;
         set => Thrift.Repetition_type = value ? Parquet.Thrift.FieldRepetitionType.OPTIONAL : Parquet.Thrift.FieldRepetitionType.REQUIRED;
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
      internal bool HasDefinitionLevelsPage
      {
         get
         {
            if (!Thrift.__isset.repetition_type)
               throw new ParquetException("repetiton type is missing");

            return Thrift.Repetition_type != Parquet.Thrift.FieldRepetitionType.REQUIRED;
         }
      }

      internal int MaxDefinitionLevel
      {
         get
         {
            int maxLevel = 0;

            //detect max repetition level for the given path
            SchemaElement se = this;
            while (se != null)
            {
               if (se.Thrift.Repetition_type != Parquet.Thrift.FieldRepetitionType.REQUIRED) maxLevel += 1;

               se = se.Parent;
            }

            return maxLevel;
         }
      }


      internal bool HasRepetitionLevelsPage => MaxRepetitionLevel > 0;

      internal int MaxRepetitionLevel
      {
         get
         {
            int maxLevel = 0;

            //detect max repetition level for the given path
            SchemaElement se = this;
            while (se != null)
            {
               if (se.Thrift.Repetition_type == Parquet.Thrift.FieldRepetitionType.REPEATED) maxLevel += 1;

               se = se.Parent;
            }

            return maxLevel;
         }
      }

      /// <summary>
      /// Pretty prints
      /// </summary>
      public override string ToString()
      {
         return $"{Name} ({ElementType}), nullable: {IsNullable}";
      }

      /// <summary>
      /// Indicates whether the current object is equal to another object of the same type.
      /// </summary>
      /// <param name="other">An object to compare with this object.</param>
      /// <returns>
      /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
      /// </returns>
      public bool Equals(SchemaElement other)
      {
         if (ReferenceEquals(null, other)) return false;
         if (ReferenceEquals(this, other)) return true;

         return string.Equals(Name, other.Name) &&
                ElementType == other.ElementType &&
                Thrift.Type.Equals(other.Thrift.Type) &&
                Thrift.__isset.converted_type == other.Thrift.__isset.converted_type &&
                Thrift.Converted_type.Equals(other.Thrift.Converted_type);
      }

      /// <summary>
      /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
      /// </summary>
      /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
      /// <returns>
      ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
      /// </returns>
      public override bool Equals(object obj)
      {
         if (ReferenceEquals(null, obj)) return false;
         if (ReferenceEquals(this, obj)) return true;
         if (obj.GetType() != GetType()) return false;

         return Equals((SchemaElement) obj);
      }

      /// <summary>
      /// Returns a hash code for this instance.
      /// </summary>
      /// <returns>
      /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
      /// </returns>
      public override int GetHashCode()
      {
         unchecked
         {
            var hashCode = (Name != null ? Name.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (ElementType != null ? ElementType.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (Thrift != null ? Thrift.GetHashCode() : 0);
            return hashCode;
         }
      }
   }
}