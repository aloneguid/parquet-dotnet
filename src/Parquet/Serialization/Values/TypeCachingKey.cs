using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   class TypeCachingKey : IEquatable<TypeCachingKey>
   {
      public TypeCachingKey(Type classType, DataField field)
      {
         ClassType = classType ?? throw new ArgumentNullException(nameof(classType));
         Field = field ?? throw new ArgumentNullException(nameof(field));
      }

      public Type ClassType { get; }

      public DataField Field { get; }

      public bool Equals(TypeCachingKey other)
      {
         if (ReferenceEquals(other, null)) return false;
         if (ReferenceEquals(other, this)) return true;

         return ClassType.Equals(other.ClassType) && Field.Equals(other.Field);
      }
      
      public override int GetHashCode()
      {
         return 31 * ClassType.GetHashCode() + Field.GetHashCode();
      }
   }
}
