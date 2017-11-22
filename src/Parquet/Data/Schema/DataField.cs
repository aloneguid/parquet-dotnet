using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
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

      public DataField(string name, DataType dataType, bool hasNulls = true, bool isArray = false) : base(name, SchemaType.PrimitiveType)
      {
         DataType = dataType;
         HasNulls = hasNulls;
         IsArray = isArray;

         IDataTypeHandler handler = DataTypeFactory.Match(dataType);
         if (handler != null)
         {
            ClrType = handler.ClrType;
         }

         Path = name;
      }

      /// <summary>
      /// Pretty prints
      /// </summary>
      public override string ToString()
      {
         return $"{Name}: {DataType} (nulls: {HasNulls}, array: {IsArray}, clr: {ClrType}, path: {Path})";
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
   }
}