using Parquet.File;
using System;
using System.Collections.Generic;
using System.Collections;
using System.Diagnostics;
using NetBox.Collections;

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

      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="children"></param>
      public SchemaElement(string name, params SchemaElement[] children) : base(name, typeof(T), children)
      {

      }
   }

   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   [DebuggerDisplay("{Name}: {ElementType} (nullable = {IsNullable}), RL: {MaxRepetitionLevel}, DL: {MaxDefinitionLevel}, P: {Path}")]
   public class SchemaElement : IEquatable<SchemaElement>
   {
      private readonly CallbackList<SchemaElement> _children = new CallbackList<SchemaElement>();
      private string _path;
      private static readonly FileMetadataBuilder Builder = new FileMetadataBuilder();

      /// <summary>
      /// Gets the children schemas. Made internal temporarily, until we can actually read nested structures.
      /// </summary>
      public IList<SchemaElement> Children => _children;

      internal void Detach()
      {
         Parent = null;
      }

      internal bool IsNestedStructure => _children.Count != 0;

      /// <summary>
      /// Gets parent schema element, if present. Null for root schema elements.
      /// </summary>
      internal SchemaElement Parent { get; private set; }

      /// <summary>
      /// Used only by derived classes implementing an edge case type
      /// </summary>
      /// <param name="name"></param>
      /// <param name="nullable"></param>
      protected SchemaElement(string name, bool nullable = false) : this()
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         Thrift = Builder.CreateSimpleSchemaElement(name, nullable);

         UpdateFlags();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="elementType">Type of the element in this column</param>
      public SchemaElement(string name, Type elementType) : this()
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         ColumnType = elementType;

         Thrift = Builder
            .CreateSchemaElement(name, elementType,
               out Type resultElementType,
               out string resultPath);

         ElementType = resultElementType;
         Path = resultPath;

         UpdateFlags();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="elementType">Type of the element in this column</param>
      /// <param name="children">Child elements</param>
      public SchemaElement(string name, Type elementType, IEnumerable<SchemaElement> children = null) : this(name, elementType)
      {
         if(children != null)
         {
            foreach(SchemaElement se in children)
            {
               if (se != null)
               {
                  _children.Add(se);
               }
            }
         }
      }

      private void UpdateFlags()
      {
         if (IsNullable)
         {
            MaxDefinitionLevel = 1;
         }

         if(IsRepeated)
         {
            MaxRepetitionLevel = 1;
         }

      }

      //todo: move this out completely into FileMetadataParser
      internal SchemaElement(Thrift.SchemaElement thriftSchema, SchemaElement parent, ParquetOptions formatOptions, Type elementType, string name = null) : this()
      {
         Name = name ?? thriftSchema.Name;
         Thrift = thriftSchema;
         Parent = parent;

         if (elementType != null)
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

      private SchemaElement()
      {
         _children.OnAdd = OnAddChild;
      }

      private SchemaElement OnAddChild(SchemaElement se)
      {
         se.Parent = this;
         se.Parent.Thrift.Num_children += 1;

         se.MaxDefinitionLevel = se.Parent.MaxDefinitionLevel + (se.IsNullable ? 1 : 0);
         se.MaxRepetitionLevel = se.Parent.MaxRepetitionLevel + (se.IsRepeated ? 1 : 0);

         return se;
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
      }

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
               (ColumnType == other.ColumnType
                  || (ColumnType == typeof(DateTime) && other.ColumnType == typeof(DateTimeOffset))) &&
               (ElementType == other.ElementType
                  || (ElementType == typeof(DateTime) && other.ElementType == typeof(DateTimeOffset))) &&
               Thrift.Type.Equals(other.Thrift.Type) &&
               Thrift.__isset.converted_type == other.Thrift.__isset.converted_type &&
               Thrift.Converted_type.Equals(other.Thrift.Converted_type) &&
               Thrift.__isset.type_length == other.Thrift.__isset.type_length &&
               Thrift.Type_length == other.Thrift.Type_length &&
               Thrift.__isset.scale == other.Thrift.__isset.scale &&
               Thrift.Scale == other.Thrift.Scale &&
               Thrift.__isset.precision == other.Thrift.__isset.precision &&
               Thrift.Precision == other.Thrift.Precision;
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