using Parquet.File;
using System;
using System.Collections.Generic;
using System.Collections;
using Parquet.File.Values.Primitives;

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
      /// <param name="parent">Parent schema element</param>
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
         ElementType = ColumnType = typeof(Interval);
      }
   }

   /// <summary>
   /// Maps to Parquet decimal type, allowing to specify custom scale and precision
   /// </summary>
   public class DecimalSchemaElement : SchemaElement
   {
      /// <summary>
      /// Constructs class instance
      /// </summary>
      /// <param name="name">The name of the column</param>
      /// <param name="precision">Cusom precision</param>
      /// <param name="scale">Custom scale</param>
      /// <param name="forceByteArrayEncoding">Whether to force decimal type encoding as fixed bytes. Hive and Impala only understands decimals when forced to true.</param>
      public DecimalSchemaElement(string name, int precision, int scale, bool forceByteArrayEncoding = false) : base(name)
      {
         if (precision < 1) throw new ArgumentException("precision cannot be less than 1", nameof(precision));
         if (scale < 1) throw new ArgumentException("scale cannot be less than 1", nameof(scale));

         Thrift.Type tt;

         if (forceByteArrayEncoding)
         {
            tt = Parquet.Thrift.Type.FIXED_LEN_BYTE_ARRAY;
         }
         else
         {
            if (precision <= 9)
               tt = Parquet.Thrift.Type.INT32;
            else if (precision <= 18)
               tt = Parquet.Thrift.Type.INT64;
            else
               tt = Parquet.Thrift.Type.FIXED_LEN_BYTE_ARRAY;
         }

         Thrift.Type = tt;
         Thrift.Converted_type = Parquet.Thrift.ConvertedType.DECIMAL;
         Thrift.Precision = precision;
         Thrift.Scale = scale;
         ElementType = ColumnType = typeof(decimal);
      }
   }


   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public class SchemaElement : IEquatable<SchemaElement>
   {
      private readonly List<SchemaElement> _children = new List<SchemaElement>();
      private string _path;
      private readonly ColumnStats _stats = new ColumnStats();

      /// <summary>
      /// Gets the children schemas. Made internal temporarily, until we can actually read nested structures.
      /// </summary>
      internal IList<SchemaElement> Children => _children;

      internal bool IsNestedStructure => _children.Count != 0;

      /// <summary>
      /// Gets parent schema element, if present. Null for root schema elements.
      /// </summary>
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
      /// <param name="parent">Parent element, or null when this element belongs to a root.</param>
      public SchemaElement(string name, Type elementType, SchemaElement parent = null)
      {
         SetProperties(name, elementType);
         TypeFactory.AdjustSchema(Thrift, ElementType);
         Parent = parent;
      }

      private void SetProperties(string name, Type elementType)
      {
         if (string.IsNullOrEmpty(name))
            throw new ArgumentException("cannot be null or empty", nameof(name));

         //todo: a lot of this stuff has to move out to schema parser

         Name = name;

         if (TypeFactory.TryExtractEnumerableType(elementType, out Type baseType))
         {
            ElementType = baseType;
            Path = FileMetadataBuilder.BuildRepeatablePath(this);
            IsRepeated = true;

            Thrift = new Thrift.SchemaElement(name)
            {
               Repetition_type = Parquet.Thrift.FieldRepetitionType.REPEATED
            };
         }
         else
         {

            ElementType = elementType;

            Thrift = new Thrift.SchemaElement(name)
            {
               //this must be changed later or if column has nulls (on write)
               Repetition_type = Parquet.Thrift.FieldRepetitionType.REQUIRED
            };
         }

         ColumnType = elementType;
      }

      internal SchemaElement(Thrift.SchemaElement thriftSchema, SchemaElement parent, ParquetOptions formatOptions, Type elementType, string name = null)
      {
         Name = name ?? thriftSchema.Name;
         Thrift = thriftSchema;
         Parent = parent;

         if(elementType != null)
         {
            ElementType = elementType;
            ColumnType = elementType;

            if (elementType == typeof(IEnumerable))
            {
               Type itemType = TypePrimitive.GetSystemTypeBySchema(this, formatOptions);
               Type ienumType = typeof(IEnumerable<>);
               Type ienumGenericType = ienumType.MakeGenericType(itemType);
               ElementType = itemType;
               ColumnType = ienumGenericType;
            }
         }
         else
         {
            ElementType = TypePrimitive.GetSystemTypeBySchema(this, formatOptions);
            ColumnType = ElementType;
         }
      }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; private set; }

      /// <summary>
      /// Element type. For simple types it's the type of the value stored in the column.
      /// For IEnumerableT it returns the T.
      /// </summary>
      public Type ElementType { get; internal set; }

      /// <summary>
      /// Type of the column.
      /// </summary>
      public Type ColumnType { get; internal set; }

      /// <summary>
      /// When true, this fields is repeated i.e. has multiple value
      /// </summary>
      public bool IsRepeated { get; internal set; }

      /// <summary>
      /// Element path, separated by dots (.)
      /// </summary>
      public string Path
      {
         get
         {
            if (_path != null) return _path;

            if (Parent == null) return Name;

            return Parent.Path + Schema.PathSeparator + Name;

         }
         internal set
         {
            _path = value;
         }
      }

      /// <summary>
      /// Returns true if element can have null values
      /// </summary>
      internal bool IsNullable
      {
         get => Thrift.Repetition_type != Parquet.Thrift.FieldRepetitionType.REQUIRED;
         set => Thrift.Repetition_type = value ? Parquet.Thrift.FieldRepetitionType.OPTIONAL : Parquet.Thrift.FieldRepetitionType.REQUIRED;
      }

      internal bool HasNulls => _stats.NullCount > 0;

      internal Thrift.SchemaElement Thrift { get; set; }

      internal bool IsAnnotatedWith(Thrift.ConvertedType ct)
      {
         //checking __isset is important, this is a way of Thrift to tell whether the variable is set at all
         return Thrift.__isset.converted_type && Thrift.Converted_type == ct;
      }

      internal bool HasDefinitionLevelsPage => MaxDefinitionLevel > 0;

      internal int MaxDefinitionLevel { get; set; }

      internal bool HasRepetitionLevelsPage => MaxRepetitionLevel > 0;

      internal int MaxRepetitionLevel { get; set; }

      internal ColumnStats Stats => _stats;

      /// <summary>
      /// Pretty prints
      /// </summary>
      public override string ToString()
      {
         return $"{Name}: {ElementType} (nullable = {IsNullable})";
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

         //todo: check equality for child elements

         return string.Equals(Name, other.Name) &&
               ColumnType == other.ColumnType &&
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
            int hashCode = (Name != null ? Name.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (ElementType != null ? ElementType.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (Thrift != null ? Thrift.GetHashCode() : 0);
            return hashCode;
         }
      }
   }
}