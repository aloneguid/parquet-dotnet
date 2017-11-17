using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data
{
   /// <summary>
   /// Represents dataset schema
   /// </summary>
   public class Schema : IEquatable<Schema>
   {
      /// <summary>
      /// Symbol used to separate path parts in schema element path
      /// </summary>
      public const string PathSeparator = ".";

      /// <summary>
      /// Character used to separate path parts in schema element path
      /// </summary>
      public const char PathSeparatorChar = '.';

      private readonly List<SchemaElement> _elements;

      /// <summary>
      /// Initializes a new instance of the <see cref="Schema"/> class from schema elements.
      /// </summary>
      /// <param name="elements">The elements.</param>
      public Schema(IEnumerable<SchemaElement> elements)
      {
         _elements = elements.ToList();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="Schema"/> class.
      /// </summary>
      /// <param name="elements">The elements.</param>
      public Schema(params SchemaElement[] elements)
      {
         _elements = elements.ToList();
      }

      /// <summary>
      /// Gets the schema elements
      /// </summary>
      public IReadOnlyList<SchemaElement> Elements => _elements;

      /// <summary>
      /// Gets the number of elements in the schema
      /// </summary>
      public int Length => _elements.Count;

      /// <summary>
      /// Gets the column names as string array
      /// </summary>
      public string[] ColumnNames => _elements.Select(e => e.Name).ToArray();

      /// <summary>
      /// Get schema element by index
      /// </summary>
      /// <param name="i">Index of schema element</param>
      /// <returns>Schema element</returns>
      public SchemaElement this[int i]
      {
         get { return _elements[i]; }
      }

      /// <summary>
      /// Get schema element by name
      /// </summary>
      /// <param name="name">Schema element name</param>
      /// <returns>Schema element</returns>
      public SchemaElement this[string name]
      {
         get
         {
            SchemaElement result = _elements.FirstOrDefault(e => e.Name == name);

            if (result == null) throw new ArgumentException($"schema element '{name}' not found", nameof(name));

            return result;
         }
      }

      /// <summary>
      /// Gets the column index by schema element
      /// </summary>
      /// <returns>Element index or -1 if not found</returns>
      public int GetElementIndex(SchemaElement schema)
      {
         for (int i = 0; i < _elements.Count; i++)
            if (schema.Equals(_elements[i])) return i;

         return -1;
      }

      /// <summary>
      /// Indicates whether the current object is equal to another object of the same type.
      /// </summary>
      /// <param name="other">An object to compare with this object.</param>
      /// <returns>
      /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
      /// </returns>
      public bool Equals(Schema other)
      {
         if (ReferenceEquals(null, other)) return false;
         if (ReferenceEquals(this, other)) return true;

         if (_elements.Count != other._elements.Count) return false;

         for(int i = 0; i < _elements.Count; i++)
         {
            if (!_elements[i].Equals(other._elements[i])) return false;
         }

         return true;
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

         return Equals((Schema) obj);
      }

      /// <summary>
      /// Returns a hash code for this instance.
      /// </summary>
      /// <returns>
      /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
      /// </returns>
      public override int GetHashCode()
      {
         return _elements.Aggregate(1, (current, se) => current * se.GetHashCode());
      }
   }
}
