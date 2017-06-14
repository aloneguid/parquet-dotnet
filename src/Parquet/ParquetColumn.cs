using System;

namespace Parquet
{
   /// <summary>
   /// Represents a column
   /// </summary>
   public class ParquetColumn : IEquatable<ParquetColumn>
   {
      public ParquetColumn(string name)
      {
         Name = name;
      }

      public string Name { get; }

      //todo: add more meta

      public override string ToString()
      {
         return Name;
      }

      public bool Equals(ParquetColumn other)
      {
         if (ReferenceEquals(other, null)) return false;
         if (ReferenceEquals(other, this)) return true;
         return other.Name == this.Name;
      }

      public override bool Equals(object obj)
      {
         if (ReferenceEquals(obj, null)) return false;
         if (obj.GetType() != typeof(ParquetColumn)) return false;
         return Equals((ParquetColumn)obj);
      }

      public override int GetHashCode()
      {
         return Name.GetHashCode();
      }
   }
}
