using System;
using System.Collections;

namespace Parquet
{
   /// <summary>
   /// Represents a column
   /// </summary>
   public class ParquetColumn : IEquatable<ParquetColumn>
   {
      public ParquetColumn(string name, IList values)
      {
         Name = name;
         Values = values;
      }

      public string Name { get; }

      public IList Values { get; }

      public void Add(ParquetColumn col)
      {
         foreach(var value in col.Values)
         {
            Values.Add(value);
         }
      }


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
