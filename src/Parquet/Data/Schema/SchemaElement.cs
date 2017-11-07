using Parquet.File;
using System;
using System.Collections.Generic;
using System.Collections;
using System.Diagnostics;

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
   [DebuggerDisplay("{Name}: {ElementType} (NL/RP/NS: {IsNullable}/{IsRepeated}/{IsNestedStructure}), RL: {MaxRepetitionLevel}, DL: {MaxDefinitionLevel}, P: {Path}")]
   public class SchemaElement : IEquatable<SchemaElement>
   {
      private readonly CallbackList<SchemaElement> _children = new CallbackList<SchemaElement>();
      private readonly List<SchemaElement> _extra = new List<SchemaElement>();
      private string _path;
      private string _pathName;
#pragma warning disable IDE1006 // Naming Styles
      private static readonly FileMetadataBuilder Builder = new FileMetadataBuilder();
#pragma warning restore IDE1006 // Naming Styles

      /// <summary>
      /// Gets the children schemas. Made internal temporarily, until we can actually read nested structures.
      /// </summary>
      public IList<SchemaElement> Children => _children;

      internal void Detach()
      {
         Parent = null;
      }

      internal bool AutoUpdateLevels { get; set; } = true;

      internal bool IsNestedStructure => _children.Count != 0;

      internal IList<SchemaElement> Extra => _extra;

      /// <summary>
      /// Gets parent schema element, if present. Null for root schema elements.
      /// </summary>
      public SchemaElement Parent { get; internal set; }

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
               out string pathName);

         ElementType = resultElementType;
         _pathName = pathName;
         IsRepeated = Thrift.Repetition_type == Parquet.Thrift.FieldRepetitionType.REPEATED;
         UpdateFlags();
      }

      internal SchemaElement(Thrift.SchemaElement tse, string nameOverride,
         Type elementType, Type columnType,
         string path) : this()
      {
         Thrift = tse;
         Name = nameOverride ?? tse.Name;
         ElementType = elementType;
         ColumnType = columnType;
         _path = path;
         IsRepeated = Thrift.Repetition_type == Parquet.Thrift.FieldRepetitionType.REPEATED;
      }

      internal IList CreateValuesList(int capacity, bool honorRepeatables = false)
      {
         if(honorRepeatables && IsRepeated)
         {
            return new List<IEnumerable>();
         }

         TypePrimitive tp = TypePrimitive.Find(ElementType);

         return tp.CreateList(capacity, IsNullable);
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
         MaxDefinitionLevel = MaxRepetitionLevel = 0;

         if (IsNullable)
         {
            MaxDefinitionLevel += 1;
         }

         if(IsRepeated)
         {
            MaxRepetitionLevel += 1;
         }

         //we don't know if the element has children here, because they are added later
      }



      private SchemaElement()
      {
         _children.OnAdd = OnAddChild;
      }

      private SchemaElement OnAddChild(SchemaElement se)
      {
         se.AutoUpdateLevels = AutoUpdateLevels;

         se.Parent = this;
         se.Parent.Thrift.Num_children = se.Parent.Children.Count + 1;

         if (AutoUpdateLevels)
         {
            if (Children.Count == 0)
            {
               //when child was added the first time, the parent is nullable, therefore definition level needs to be upped
               MaxDefinitionLevel += 1;

               //if we are repeatable, there is another wired element with optional flag, therefore up one level up
               if (IsRepeated)
               {
                  MaxDefinitionLevel += 1;
               }
            }

            se.MaxDefinitionLevel = MaxDefinitionLevel + se.MaxDefinitionLevel;
            se.MaxRepetitionLevel = MaxRepetitionLevel + se.MaxRepetitionLevel;
         }

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
      /// When true, this fields is repeated i.e. has multiple values
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

            string pp = _pathName ?? Name;

            if (Parent == null) return pp;

            return (Parent.Path == string.Empty)
               ? pp
               : Parent.Path + Schema.PathSeparator + pp;

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