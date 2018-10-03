using System;
using System.Collections;
using Parquet.File;

namespace Parquet.Data
{
   /// <summary>
   /// Field containing actual data, unlike fields containing metadata.
   /// </summary>
   public class DataField : Field, IEquatable<DataField>
   {
      /// <summary>
      /// Data type of this element
      /// </summary>
      public DataType DataType { get; }

      /// <summary>
      /// When true, this element is allowed to have nulls
      /// </summary>
      public bool HasNulls { get; }

      /// <summary>
      /// When true, the value is an array rather than a single value.
      /// </summary>
      public bool IsArray { get; }

      /// <summary>
      /// CLR type of this column. Not sure whether to expose this externally yet.
      /// </summary>
      internal Type ClrType { get; private set; }

      internal Type ClrNullableIfHasNullsType { get; private set; }

      /// <summary>
      /// Creates a new instance of <see cref="DataField"/> by name and CLR type.
      /// </summary>
      /// <param name="name">Field name</param>
      /// <param name="clrType">CLR type of this field. The type is internally discovered and expanded into appropriate Parquet flags.</param>
      public DataField(string name, Type clrType) 
         : this(name,
              Discover(clrType).dataType,
              Discover(clrType).hasNulls,
              Discover(clrType).isArray)
      {
         //todo: calls to Discover() can be killed by making a constructor method
      }

      /// <summary>
      /// Creates a new instance of <see cref="DataField"/> by specifying all the required attributes.
      /// </summary>
      /// <param name="name">Field name.</param>
      /// <param name="dataType">Native Parquet type</param>
      /// <param name="hasNulls">When true, the field accepts null values. Note that nullable values take slightly more disk space comparing to non-nullable.</param>
      /// <param name="isArray">When true, each value of this field can have multiple values, similar to array in .NET</param>
      public DataField(string name, DataType dataType, bool hasNulls = true, bool isArray = false) : base(name, SchemaType.Data)
      {
         DataType = dataType;
         HasNulls = hasNulls;
         IsArray = isArray;

         MaxRepetitionLevel = isArray ? 1 : 0;

         IDataTypeHandler handler = DataTypeFactory.Match(dataType);
         if (handler != null)
         {
            ClrType = handler.ClrType;
            ClrNullableIfHasNullsType = hasNulls ? ClrType.GetNullable() : ClrType;
         }
      }

      internal override string PathPrefix
      {
         set
         {
            Path = value.AddPath(Name);
         }
      }

      /// <summary>
      /// see
      /// <see cref="ThriftFooter.GetLevels(Thrift.ColumnChunk, out int, out int)"/>
      /// and
      /// <see cref="BasicDataTypeHandler{TSystemType}.CreateSchemaElement(System.Collections.Generic.IList{Thrift.SchemaElement}, ref int, out int)"/>
      /// </summary>
      internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel)
      {
         MaxRepetitionLevel = parentRepetitionLevel + (IsArray ? 1 : 0);
         MaxDefinitionLevel = parentDefinitionLevel + (HasNulls ? 1 : 0);
      }

      /// <summary>
      /// Indicates whether the current object is equal to another object of the same type.
      /// </summary>
      /// <param name="other">An object to compare with this object.</param>
      /// <returns>
      /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
      /// </returns>
      public bool Equals(DataField other)
      {
         if (ReferenceEquals(null, other)) return false;
         if (ReferenceEquals(this, other)) return true;

         //todo: check equality for child elements

         return
            string.Equals(Name, other.Name) &&
            DataType.Equals(other.DataType) &&
            HasNulls == other.HasNulls &&
            IsArray == other.IsArray;
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
         if (!(obj is DataField)) return false;

         return Equals((DataField)obj);
      }

      /// <summary>
      /// Returns a hash code for this instance.
      /// </summary>
      /// <returns>
      /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
      /// </returns>
      public override int GetHashCode()
      {
         return Name.GetHashCode() * DataType.GetHashCode();
      }

      #region [ Type Resolution ]

      private struct CInfo
      {
         public DataType dataType;
         public Type baseType;
         public bool isArray;
         public bool hasNulls;
      }

      private static CInfo Discover(Type t)
      {
         Type baseType = t;
         bool isArray = false;
         bool hasNulls = false;

         //throw a useful hint
         if (t.TryExtractDictionaryType(out Type dKey, out Type dValue))
         {
            throw new ArgumentException($"cannot declare a dictionary this way, please use {nameof(MapField)}.");
         }

         if (t.TryExtractEnumerableType(out Type enumItemType))
         {
            baseType = enumItemType;
            isArray = true;
         }

         if (baseType.IsNullable())
         {
            baseType = baseType.GetNonNullable();
            hasNulls = true;
         }

         IDataTypeHandler handler = DataTypeFactory.Match(baseType);
         if (handler == null) DataTypeFactory.ThrowClrTypeNotSupported(baseType);

         return new CInfo
         {
            dataType = handler.DataType,
            baseType = baseType,
            isArray = isArray,
            hasNulls = hasNulls
         };
      }

      #endregion
   }
}